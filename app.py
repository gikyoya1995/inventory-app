import csv
import datetime
import io
import json
import os
import secrets
import smtplib
import sqlite3
import time
import unicodedata
import urllib.parse
from email.mime.text import MIMEText

import requests
from flask import Flask, flash, redirect, render_template, request, url_for
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-change-in-prod")

DATABASE = os.path.join(os.path.dirname(__file__), "inventory.db")

# メール設定（環境変数で上書き可能）
MAIL_HOST = os.environ.get("MAIL_HOST", "smtp.gmail.com")
MAIL_PORT = int(os.environ.get("MAIL_PORT", "587"))
MAIL_USER = os.environ.get("MAIL_USER", "")
MAIL_PASS = os.environ.get("MAIL_PASS", "")
MAIL_FROM = os.environ.get("MAIL_FROM", "")
MAIL_TO = os.environ.get("MAIL_TO", "")
STOCK_ALERT_THRESHOLD = 10

# ─── カラーミーショップ API 定数 ──────────────────────────────────────────────────
COLORME_AUTHORIZE_URL = "https://api.shop-pro.jp/oauth/authorize"
COLORME_TOKEN_URL     = "https://api.shop-pro.jp/oauth/token"
COLORME_API_BASE      = "https://api.shop-pro.jp/v1"


# ─── DB ───────────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


RESTRICTED_KEYWORDS = ["仙禽", "花邑"]


def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                product_code TEXT UNIQUE NOT NULL,
                name         TEXT NOT NULL,
                stock        INTEGER NOT NULL DEFAULT 0,
                price        INTEGER NOT NULL DEFAULT 0,
                is_restricted INTEGER NOT NULL DEFAULT 0,
                updated_at   DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # 既存DBへの列追加（初回以降の起動でも安全）
        try:
            conn.execute("ALTER TABLE products ADD COLUMN is_restricted INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass

        conn.execute("""
            CREATE TABLE IF NOT EXISTS monthly_orders (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_code TEXT NOT NULL,
                product_code  TEXT NOT NULL,
                order_year    INTEGER NOT NULL,
                order_month   INTEGER NOT NULL,
                created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.commit()


def normalize_code(s) -> str:
    """商品コード照合用の正規化：前後スペース除去＋全角英数字→半角。None/空文字は空文字を返す。"""
    if not s:
        return ""
    return unicodedata.normalize("NFKC", str(s).strip())


def is_restricted_product(name: str) -> bool:
    return any(kw in name for kw in RESTRICTED_KEYWORDS)


# ─── 設定ヘルパー ──────────────────────────────────────────────────────────────

def get_setting(key: str, default: str = "") -> str:
    with get_db() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key: str, value: str) -> None:
    with get_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value)
        )
        conn.commit()


def delete_setting(key: str) -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM settings WHERE key=?", (key,))
        conn.commit()


# ─── カラーミー API ヘルパー ───────────────────────────────────────────────────

def colorme_is_connected() -> bool:
    return bool(get_setting("access_token"))


def colorme_headers() -> dict:
    token = get_setting("access_token")
    return {
        "Authorization":  f"Bearer {token}",
        "Content-Type":   "application/json",
        "Cache-Control":  "no-cache, no-store",
        "Pragma":         "no-cache",
    }


def colorme_get_all_products() -> list:
    """カラーミー全商品を50件ずつページングで全件取得する。
    ページ間に1秒待機してレート制限を回避。
    取得件数 < limit になったら最終ページと判定して終了。
    """
    http = requests.Session()
    http.headers.update(colorme_headers())

    products = []
    limit    = 50   # カラーミーAPIの最大値
    offset   = 0
    page     = 1

    try:
        while True:
            app.logger.info(f"カラーミーAPI: ページ{page}取得開始 (offset={offset})")
            try:
                resp = http.get(
                    f"{COLORME_API_BASE}/products.json",
                    params={"limit": limit, "offset": offset},
                    timeout=60,
                )
                app.logger.info(f"カラーミーAPI: ページ{page} ステータス={resp.status_code}")
                resp.raise_for_status()
            except Exception as e:
                app.logger.error(f"カラーミーAPI: ページ{page}でエラー (offset={offset}): {e}")
                raise

            data  = resp.json()
            batch = data.get("products", [])
            products.extend(batch)
            app.logger.info(
                f"カラーミーAPI: ページ{page}完了 取得={len(batch)}件 累計={len(products)}件"
            )

            # 取得件数が limit 未満なら最終ページ
            if len(batch) < limit:
                app.logger.info(f"カラーミーAPI: ページ{page}が最終ページ。全件取得完了。")
                break

            offset += limit
            page   += 1
            time.sleep(1)  # レート制限回避
    finally:
        http.close()

    return products


# ─── メール通知 ────────────────────────────────────────────────────────────────

def send_alert_email(low_stock_products):
    """在庫数が閾値以下の商品一覧をメール通知する。MAIL_USER未設定時はスキップ。"""
    if not MAIL_USER:
        app.logger.warning("MAIL_USER が未設定のためメール送信をスキップしました。")
        return

    lines = [f"以下の商品の在庫が {STOCK_ALERT_THRESHOLD} 個以下になりました。\n"]
    for p in low_stock_products:
        lines.append(f"  [{p['product_code']}] {p['name']} : {p['stock']} 個")

    body = "\n".join(lines)
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = "【在庫アラート】在庫不足の商品があります"
    msg["From"] = MAIL_FROM or MAIL_USER
    msg["To"] = MAIL_TO

    try:
        with smtplib.SMTP(MAIL_HOST, MAIL_PORT) as smtp:
            smtp.starttls()
            smtp.login(MAIL_USER, MAIL_PASS)
            smtp.sendmail(msg["From"], [MAIL_TO], msg.as_string())
        app.logger.info("在庫アラートメールを送信しました。")
    except Exception as e:
        app.logger.error(f"メール送信失敗: {e}")


def check_and_alert(conn):
    """在庫が閾値以下の商品を検索してアラートメールを送る。"""
    rows = conn.execute(
        "SELECT * FROM products WHERE stock <= ?", (STOCK_ALERT_THRESHOLD,)
    ).fetchall()
    if rows:
        send_alert_email(rows)
    return rows


# ─── ルート：商品一覧 ──────────────────────────────────────────────────────────

@app.route("/")
def index():
    with get_db() as conn:
        products = conn.execute(
            "SELECT * FROM products ORDER BY updated_at DESC"
        ).fetchall()
    return render_template("index.html", products=products, threshold=STOCK_ALERT_THRESHOLD,
                           restricted_keywords=RESTRICTED_KEYWORDS)


# ─── ルート：商品追加 ──────────────────────────────────────────────────────────

@app.route("/products/add", methods=["GET", "POST"])
def add_product():
    if request.method == "POST":
        code = request.form["product_code"].strip()
        name = request.form["name"].strip()
        stock = int(request.form["stock"])
        price = int(request.form["price"])

        if not code or not name:
            flash("商品コードと商品名は必須です。", "danger")
            return render_template("product_form.html", action="add", product=request.form)

        restricted = 1 if is_restricted_product(name) else 0
        try:
            with get_db() as conn:
                conn.execute(
                    "INSERT INTO products (product_code, name, stock, price, is_restricted) VALUES (?, ?, ?, ?, ?)",
                    (code, name, stock, price, restricted),
                )
                conn.commit()
                check_and_alert(conn)
            flash(f"「{name}」を追加しました。", "success")
        except sqlite3.IntegrityError:
            flash(f"商品コード「{code}」はすでに存在します。", "danger")
        return redirect(url_for("index"))

    return render_template("product_form.html", action="add", product={})


# ─── ルート：商品編集 ──────────────────────────────────────────────────────────

@app.route("/products/<int:product_id>/edit", methods=["GET", "POST"])
def edit_product(product_id):
    with get_db() as conn:
        product = conn.execute(
            "SELECT * FROM products WHERE id = ?", (product_id,)
        ).fetchone()

    if product is None:
        flash("商品が見つかりません。", "danger")
        return redirect(url_for("index"))

    if request.method == "POST":
        name = request.form["name"].strip()
        stock = int(request.form["stock"])
        price = int(request.form["price"])
        restricted = 1 if is_restricted_product(name) else 0

        with get_db() as conn:
            conn.execute(
                "UPDATE products SET name=?, stock=?, price=?, is_restricted=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (name, stock, price, restricted, product_id),
            )
            conn.commit()
            check_and_alert(conn)
        flash(f"「{name}」を更新しました。", "success")
        return redirect(url_for("index"))

    return render_template("product_form.html", action="edit", product=product)


# ─── ルート：商品削除 ──────────────────────────────────────────────────────────

@app.route("/products/<int:product_id>/delete", methods=["POST"])
def delete_product(product_id):
    with get_db() as conn:
        product = conn.execute(
            "SELECT name FROM products WHERE id = ?", (product_id,)
        ).fetchone()
        if product:
            conn.execute("DELETE FROM products WHERE id = ?", (product_id,))
            conn.commit()
            flash(f"「{product['name']}」を削除しました。", "success")
        else:
            flash("商品が見つかりません。", "danger")
    return redirect(url_for("index"))


# ─── ルート：CSVアップロード ───────────────────────────────────────────────────

@app.route("/upload", methods=["GET", "POST"])
def upload_csv():
    result = None

    if request.method == "POST":
        if "csv_file" not in request.files or request.files["csv_file"].filename == "":
            flash("CSVファイルを選択してください。", "danger")
            return redirect(request.url)

        file = request.files["csv_file"]
        if not file.filename.endswith(".csv"):
            flash("CSVファイル（.csv）を選択してください。", "danger")
            return redirect(request.url)

        # Shift-JIS（BPS-60出力）→ UTF-8 のフォールバック付きデコード
        raw = file.stream.read()
        for encoding in ("utf-8-sig", "shift_jis", "cp932"):
            try:
                stream = io.StringIO(raw.decode(encoding))
                break
            except UnicodeDecodeError:
                continue
        else:
            flash("CSVのエンコーディングを判別できませんでした。UTF-8 または Shift-JIS で保存してください。", "danger")
            return redirect(request.url)

        reader = csv.DictReader(stream)

        updated = []
        skipped_jushidai = []
        skipped_zero = []
        skipped_unknown = []
        errors = []

        # BPS-60 列名定義
        BPS60_COLS = [
            "商品コード", "商品名", "容量", "入数",
            "売上ｹｰｽ数", "売上ﾊﾞﾗ数", "売上換算数",
            "在庫ｹｰｽ数", "在庫ﾊﾞﾗ数", "在庫換算数",
            "在庫単価", "在庫金額", "構成比",
        ]

        with get_db() as conn:
            for row_num, row in enumerate(reader, start=2):
                # 列名の正規化（空白トリム）
                row = {k.strip(): v.strip() for k, v in row.items() if k}

                name = row.get("商品名", "")
                code = row.get("商品コード", "")

                # BPS-60形式：在庫換算数を在庫として使用
                # 旧フォーマット（在庫数）にも後方互換
                stock_str = row.get("在庫換算数") or row.get("在庫数", "")
                # 在庫単価を価格として使用（なければ0）
                price_str = row.get("在庫単価") or row.get("価格", "0")

                # 十四代チェック
                if "十四代" in name:
                    skipped_jushidai.append(name or code)
                    continue

                # 在庫換算数バリデーション
                try:
                    stock = int(float(stock_str)) if stock_str else 0
                except ValueError:
                    errors.append(f"行{row_num}: 在庫換算数「{stock_str}」が数値ではありません。")
                    continue

                # 在庫0スキップ
                if stock == 0:
                    skipped_zero.append(name or code)
                    continue

                # 価格（カンマ区切り対応）
                try:
                    price = int(float(price_str.replace(",", ""))) if price_str else 0
                except ValueError:
                    price = 0

                if not code:
                    errors.append(f"行{row_num}: 商品コードが空です。")
                    continue

                # 仙禽・花邑は購入制限フラグを自動設定
                restricted = 1 if is_restricted_product(name) else 0

                # UPSERT
                existing = conn.execute(
                    "SELECT id FROM products WHERE product_code = ?", (code,)
                ).fetchone()

                if existing:
                    conn.execute(
                        """UPDATE products
                           SET name=?, stock=?, price=?, is_restricted=?, updated_at=CURRENT_TIMESTAMP
                           WHERE product_code=?""",
                        (name, stock, price, restricted, code),
                    )
                else:
                    if not name:
                        skipped_unknown.append(code)
                        continue
                    conn.execute(
                        "INSERT INTO products (product_code, name, stock, price, is_restricted) VALUES (?, ?, ?, ?, ?)",
                        (code, name, stock, price, restricted),
                    )
                updated.append(name or code)

            conn.commit()
            alerted = check_and_alert(conn)

        result = {
            "updated": updated,
            "skipped_jushidai": skipped_jushidai,
            "skipped_zero": skipped_zero,
            "skipped_unknown": skipped_unknown,
            "errors": errors,
            "alerted": alerted,
        }

        if updated:
            flash(f"{len(updated)} 件の在庫を更新しました。", "success")
        if alerted:
            flash(f"{len(alerted)} 件の商品が在庫アラート対象です。", "warning")

    return render_template("upload.html", result=result)


# ─── ルート：月次注文制限チェック・記録 ───────────────────────────────────────────

@app.route("/orders", methods=["GET", "POST"])
def orders():
    import datetime
    now = datetime.datetime.now()
    check_result = None

    if request.method == "POST":
        action = request.form.get("action")
        customer_code = request.form.get("customer_code", "").strip()
        product_code = request.form.get("product_code", "").strip()

        if not customer_code or not product_code:
            flash("お客様コードと商品コードを入力してください。", "danger")
        else:
            with get_db() as conn:
                product = conn.execute(
                    "SELECT * FROM products WHERE product_code = ?", (product_code,)
                ).fetchone()

                if not product:
                    flash(f"商品コード「{product_code}」が見つかりません。", "danger")
                elif not product["is_restricted"]:
                    flash(f"「{product['name']}」は注文制限対象外の商品です。", "info")
                else:
                    existing_order = conn.execute(
                        """SELECT COUNT(*) as cnt FROM monthly_orders
                           WHERE customer_code=? AND product_code=?
                             AND order_year=? AND order_month=?""",
                        (customer_code, product_code, now.year, now.month),
                    ).fetchone()

                    if action == "check":
                        if existing_order["cnt"] >= 1:
                            check_result = {
                                "ok": False,
                                "message": (
                                    f"お客様コード「{customer_code}」は今月すでに"
                                    f"「{product['name']}」を注文済みです。"
                                    f"（制限：月1本まで）"
                                ),
                                "product": product,
                                "customer_code": customer_code,
                            }
                        else:
                            check_result = {
                                "ok": True,
                                "message": f"「{product['name']}」の注文が可能です。（今月の注文: 0本）",
                                "product": product,
                                "customer_code": customer_code,
                            }

                    elif action == "record":
                        if existing_order["cnt"] >= 1:
                            flash(
                                f"エラー：お客様コード「{customer_code}」は今月すでに"
                                f"「{product['name']}」を注文済みです。注文を受け付けできません。",
                                "danger",
                            )
                        else:
                            conn.execute(
                                """INSERT INTO monthly_orders (customer_code, product_code, order_year, order_month)
                                   VALUES (?, ?, ?, ?)""",
                                (customer_code, product_code, now.year, now.month),
                            )
                            conn.commit()
                            flash(
                                f"「{product['name']}」の注文を記録しました。（お客様: {customer_code}）",
                                "success",
                            )

    # 今月の注文一覧
    with get_db() as conn:
        monthly_records = conn.execute(
            """SELECT mo.*, p.name as product_name
               FROM monthly_orders mo
               LEFT JOIN products p ON mo.product_code = p.product_code
               WHERE mo.order_year=? AND mo.order_month=?
               ORDER BY mo.created_at DESC""",
            (now.year, now.month),
        ).fetchall()
        restricted_products = conn.execute(
            "SELECT * FROM products WHERE is_restricted=1 ORDER BY name"
        ).fetchall()

    return render_template(
        "orders.html",
        monthly_records=monthly_records,
        restricted_products=restricted_products,
        check_result=check_result,
        current_year=now.year,
        current_month=now.month,
    )


@app.route("/orders/<int:order_id>/delete", methods=["POST"])
def delete_order(order_id):
    with get_db() as conn:
        conn.execute("DELETE FROM monthly_orders WHERE id = ?", (order_id,))
        conn.commit()
    flash("注文記録を削除しました。", "success")
    return redirect(url_for("orders"))


# ─── ルート：設定画面 ──────────────────────────────────────────────────────────

@app.route("/settings", methods=["GET", "POST"])
def settings():
    if request.method == "POST":
        client_id     = request.form.get("client_id", "").strip()
        client_secret = request.form.get("client_secret", "").strip()
        redirect_uri  = request.form.get("redirect_uri", "").strip()

        if client_id:
            set_setting("client_id", client_id)
        if client_secret:
            set_setting("client_secret", client_secret)
        if redirect_uri:
            set_setting("redirect_uri", redirect_uri)

        flash("設定を保存しました。", "success")
        return redirect(url_for("settings"))

    return render_template(
        "settings.html",
        client_id=get_setting("client_id"),
        client_secret=get_setting("client_secret"),
        redirect_uri=get_setting("redirect_uri", "http://localhost:5001/oauth/callback"),
        is_connected=colorme_is_connected(),
        access_token_preview=get_setting("access_token")[:8] + "…" if get_setting("access_token") else "",
    )


# ─── ルート：OAuth 認証フロー ──────────────────────────────────────────────────

@app.route("/oauth/start")
def oauth_start():
    client_id    = get_setting("client_id")
    redirect_uri = get_setting("redirect_uri", "http://localhost:5001/oauth/callback")

    if not client_id:
        flash("先にクライアントIDを設定してください。", "danger")
        return redirect(url_for("settings"))

    state = secrets.token_urlsafe(16)
    set_setting("oauth_state", state)

    params = {
        "client_id":     client_id,
        "response_type": "code",
        "scope":         "read_products write_products",
        "redirect_uri":  redirect_uri,
        "state":         state,
    }
    auth_url = COLORME_AUTHORIZE_URL + "?" + urllib.parse.urlencode(params)
    return redirect(auth_url)


@app.route("/oauth/callback")
def oauth_callback():
    error = request.args.get("error")
    if error:
        flash(f"認証がキャンセルされました: {error}", "danger")
        return redirect(url_for("settings"))

    # state検証（CSRF防止）
    state          = request.args.get("state", "")
    expected_state = get_setting("oauth_state")
    delete_setting("oauth_state")
    if not expected_state or state != expected_state:
        flash("不正なリクエストです（state不一致）。もう一度お試しください。", "danger")
        return redirect(url_for("settings"))

    code         = request.args.get("code")
    client_id    = get_setting("client_id")
    client_secret = get_setting("client_secret")
    redirect_uri = get_setting("redirect_uri", "http://localhost:5001/oauth/callback")

    if not code:
        flash("認証コードが取得できませんでした。", "danger")
        return redirect(url_for("settings"))

    # アクセストークン取得
    try:
        resp = requests.post(
            COLORME_TOKEN_URL,
            data={
                "grant_type":    "authorization_code",
                "client_id":     client_id,
                "client_secret": client_secret,
                "code":          code,
                "redirect_uri":  redirect_uri,
            },
            timeout=15,
        )
        resp.raise_for_status()
        token_data = resp.json()
    except requests.RequestException as e:
        flash(f"トークン取得に失敗しました: {e}", "danger")
        return redirect(url_for("settings"))

    access_token  = token_data.get("access_token", "")
    refresh_token = token_data.get("refresh_token", "")

    if not access_token:
        flash(f"アクセストークンが取得できませんでした。レスポンス: {token_data}", "danger")
        return redirect(url_for("settings"))

    set_setting("access_token",  access_token)
    if refresh_token:
        set_setting("refresh_token", refresh_token)

    flash("カラーミーショップと連携しました。", "success")
    return redirect(url_for("settings"))


@app.route("/oauth/disconnect", methods=["POST"])
def oauth_disconnect():
    delete_setting("access_token")
    delete_setting("refresh_token")
    flash("カラーミーショップとの連携を解除しました。", "info")
    return redirect(url_for("settings"))


# ─── ルート：在庫同期 ──────────────────────────────────────────────────────────

@app.route("/sync")
def sync():
    if not colorme_is_connected():
        flash("先にカラーミーショップと連携してください。", "warning")
        return redirect(url_for("settings"))

    last_result_json = get_setting("last_sync_result", "")
    last_result = json.loads(last_result_json) if last_result_json else None

    return render_template(
        "sync.html",
        last_push=get_setting("last_sync_push"),
        last_pull=get_setting("last_sync_pull"),
        last_result=last_result,
    )


@app.route("/sync/variants")
def sync_variants():
    """カラーミー全商品のうち型番が設定されているオプションだけを一覧表示。"""
    if not colorme_is_connected():
        flash("カラーミーショップと連携されていません。", "warning")
        return redirect(url_for("settings"))

    variants_with_model = []
    error = None

    try:
        cm_products = colorme_get_all_products()
        for cm_p in cm_products:
            variants = cm_p.get("variants", [])
            if not isinstance(variants, list):
                continue
            for variant in variants:
                if not isinstance(variant, dict):
                    continue
                model_number = variant.get("model_number", "")
                if model_number:
                    variants_with_model.append({
                        "product_id":    cm_p.get("id"),
                        "product_name":  cm_p.get("name", ""),
                        "model_number":  model_number,
                        "option1_value": variant.get("option1_value", ""),
                        "option2_value": variant.get("option2_value", ""),
                        "stocks":        variant.get("stocks", 0),
                    })
    except Exception as e:
        error = str(e)

    # ローカル商品コードとの照合
    with get_db() as conn:
        local_codes = {
            row["product_code"] for row in
            conn.execute("SELECT product_code FROM products").fetchall()
        }

    return render_template(
        "sync_variants.html",
        variants=variants_with_model,
        local_codes=local_codes,
        error=error,
    )


@app.route("/sync/raw")
def sync_raw():
    """生のAPIレスポンスを確認するデバッグ用エンドポイント。"""
    if not colorme_is_connected():
        return {"error": "未連携"}, 403

    out = {}

    # 1. 商品リスト（先頭1件）の生データ
    try:
        resp = requests.get(
            f"{COLORME_API_BASE}/products.json",
            headers=colorme_headers(),
            params={"limit": 1, "offset": 0},
            timeout=15,
        )
        out["products_status"] = resp.status_code
        data = resp.json()
        products = data.get("products", [])
        out["products_raw_first"] = products[0] if products else None

        # 2. 最初の商品の個別取得（フル詳細）
        if products:
            pid = products[0]["id"]
            resp2 = requests.get(
                f"{COLORME_API_BASE}/products/{pid}.json",
                headers=colorme_headers(),
                timeout=15,
            )
            out["product_detail_status"] = resp2.status_code
            out["product_detail_raw"] = resp2.json()

            # 3. stocks サブリソース
            resp3 = requests.get(
                f"{COLORME_API_BASE}/products/{pid}/stocks.json",
                headers=colorme_headers(),
                timeout=15,
            )
            out["stocks_status"] = resp3.status_code
            out["stocks_raw"] = resp3.json()

    except Exception as e:
        out["exception"] = str(e)

    from flask import Response
    return Response(
        json.dumps(out, ensure_ascii=False, indent=2),
        mimetype="application/json",
    )


@app.route("/sync/debug")
def sync_debug():
    """マッチング確認用：カラーミーの最初の3オプションとローカルの最初の3商品コードを表示。"""
    if not colorme_is_connected():
        flash("カラーミーショップと連携されていません。", "danger")
        return redirect(url_for("settings"))

    debug = {"cm_stocks": [], "local_products": [], "error": None}

    try:
        cm_products = colorme_get_all_products()
        count = 0
        for cm_p in cm_products:
            variants = cm_p.get("variants", [])
            if not isinstance(variants, list):
                continue
            for variant in variants:
                if not isinstance(variant, dict):
                    continue
                debug["cm_stocks"].append({
                    "product_id":    cm_p.get("id"),
                    "product_name":  cm_p.get("name"),
                    "model_number":  variant.get("model_number"),
                    "option1_value": variant.get("option1_value"),
                    "option2_value": variant.get("option2_value"),
                    "stocks":        variant.get("stocks"),
                })
                count += 1
                if count >= 3:
                    break
            if count >= 3:
                break
    except Exception as e:
        debug["error"] = str(e)

    with get_db() as conn:
        rows = conn.execute("SELECT product_code, name FROM products LIMIT 3").fetchall()
        debug["local_products"] = [{"product_code": r["product_code"], "name": r["name"]} for r in rows]

    app.logger.info("=== SYNC DEBUG ===")
    app.logger.info(f"カラーミー オプション(先頭3件): {json.dumps(debug['cm_stocks'], ensure_ascii=False, indent=2)}")
    app.logger.info(f"ローカル 商品コード(先頭3件): {json.dumps(debug['local_products'], ensure_ascii=False, indent=2)}")
    app.logger.info("==================")

    return render_template("sync_debug.html", debug=debug)


@app.route("/sync/push", methods=["POST"])
def sync_push():
    """ローカルの在庫数をカラーミーショップに反映する。"""
    if not colorme_is_connected():
        flash("カラーミーショップと連携されていません。", "danger")
        return redirect(url_for("settings"))

    try:
        cm_products = colorme_get_all_products()
    except requests.RequestException as e:
        flash(f"カラーミーからの商品取得に失敗しました: {e}", "danger")
        return redirect(url_for("sync"))

    # normalize_code(model_number) -> {product_id, option_id, product_name} のインデックスを構築
    cm_variant_index = {}
    for cm_p in cm_products:
        variants = cm_p.get("variants", [])
        if not isinstance(variants, list):
            continue
        for variant in variants:
            if not isinstance(variant, dict):
                continue
            model_num = normalize_code(variant.get("model_number"))
            if model_num:
                cm_variant_index[model_num] = {
                    "product_id":   cm_p["id"],
                    "option_id":    variant.get("id"),
                    "product_name": cm_p.get("name", ""),
                }

    with get_db() as conn:
        local_products = conn.execute("SELECT * FROM products").fetchall()

    updated   = []
    skipped   = []
    not_found = []
    errors    = []

    for lp in local_products:
        code  = normalize_code(lp["product_code"])
        match = cm_variant_index.get(code)

        if not match:
            not_found.append({"code": lp["product_code"], "name": lp["name"]})
            continue

        option_id  = match.get("option_id")
        product_id = match["product_id"]

        if not option_id:
            # option_id が取得できない場合はスキップ
            app.logger.warning(f"sync_push: option_id未取得のためスキップ code={lp['product_code']}")
            skipped.append({"code": lp["product_code"], "name": lp["name"], "reason": "option_id未取得"})
            continue

        try:
            resp = requests.put(
                f"{COLORME_API_BASE}/products/{product_id}/options/{option_id}.json",
                headers=colorme_headers(),
                json={"option": {"stock": lp["stock"]}},
                timeout=60,
            )
            resp.raise_for_status()
            updated.append({"code": lp["product_code"], "name": lp["name"], "stock": lp["stock"]})
            app.logger.info(f"sync_push: 更新成功 code={lp['product_code']} stock={lp['stock']}")
        except requests.RequestException as e:
            app.logger.error(f"sync_push: 更新失敗 code={lp['product_code']} error={e}")
            errors.append({"code": lp["product_code"], "name": lp["name"], "error": str(e)})

    result = {
        "direction": "push",
        "updated":   updated,
        "skipped":   skipped,
        "not_found": not_found,
        "errors":    errors,
    }
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    set_setting("last_sync_push",   now_str)
    set_setting("last_sync_result", json.dumps(result, ensure_ascii=False))

    flash(f"プッシュ完了：{len(updated)} 件更新、{len(not_found)} 件未マッチ、{len(errors)} 件エラー", "success" if not errors else "warning")
    return redirect(url_for("sync"))


@app.route("/sync/pull", methods=["POST"])
def sync_pull():
    """カラーミーショップの在庫数をローカルに反映する。"""
    if not colorme_is_connected():
        flash("カラーミーショップと連携されていません。", "danger")
        return redirect(url_for("settings"))

    try:
        cm_products = colorme_get_all_products()
    except requests.RequestException as e:
        flash(f"カラーミーからの商品取得に失敗しました: {e}", "danger")
        return redirect(url_for("sync"))

    updated   = []
    not_found = []

    with get_db() as conn:
        for cm_p in cm_products:
            product_name = cm_p.get("name", "")
            variants = cm_p.get("variants", [])
            if not isinstance(variants, list):
                continue

            for variant in variants:
                if not isinstance(variant, dict):
                    continue

                code     = variant.get("model_number", "")
                quantity = variant.get("stocks", 0)
                if not code:
                    continue

                existing = conn.execute(
                    "SELECT id FROM products WHERE product_code=?", (code,)
                ).fetchone()

                if existing:
                    conn.execute(
                        "UPDATE products SET stock=?, updated_at=CURRENT_TIMESTAMP WHERE product_code=?",
                        (quantity, code),
                    )
                    updated.append({"code": code, "name": product_name, "stock": quantity})
                else:
                    not_found.append({"code": code, "name": product_name})

        conn.commit()
        check_and_alert(conn)

    result = {
        "direction": "pull",
        "updated":   updated,
        "not_found": not_found,
        "errors":    [],
    }
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    set_setting("last_sync_pull",   now_str)
    set_setting("last_sync_result", json.dumps(result, ensure_ascii=False))

    flash(f"プル完了：{len(updated)} 件更新、{len(not_found)} 件はローカル未登録（スキップ）", "success")
    return redirect(url_for("sync"))


# ─── エントリポイント ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5001))
    app.run(debug=True, port=port)
