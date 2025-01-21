# Databricks notebook source
# DBTITLE 1,パラメーターの設定
# Widgetsの作成
dbutils.widgets.text("catalog", "", "カタログ名")
dbutils.widgets.text("new_schema", "bricksmart", "新規スキーマ名")
dbutils.widgets.text("existing_schema", "bricksmart", "既存スキーマ名")

# Widgetからの値の取得
catalog = dbutils.widgets.get("catalog")
new_schema = dbutils.widgets.get("new_schema")
existing_schema = dbutils.widgets.get("existing_schema")

# COMMAND ----------

# DBTITLE 1,パラメーターのチェック
print(f"catalog: {catalog}")
print(f"new_schema: {new_schema}")
print(f"existing_schema: {existing_schema}")

if not catalog:
    raise ValueError("存在するカタログ名を入力してください。")
if not new_schema:
    raise ValueError("新規スキーマ名を入力してください。")

# COMMAND ----------

# DBTITLE 1,カタログ指定・スキーマのリセット
# 新規スキーマを指定する場合
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"DROP SCHEMA IF EXISTS {new_schema} CASCADE;")
spark.sql(f"CREATE SCHEMA if not exists {new_schema}")
spark.sql(f"USE SCHEMA {new_schema}")

# 既存スキーマを指定する場合
# spark.sql(f"USE CATALOG {catalog}")
# spark.sql(f"USE SCHEMA {existing_schema}")

# COMMAND ----------

# DBTITLE 1,users, products, transactions, feedbacksテーブルの生成
from pyspark.sql.functions import udf, expr, when, col, lit, round, rand, greatest, least, date_format, dayofweek
from pyspark.sql.types import StringType

import datetime
import random
import string

def generate_username():
    # 5文字のランダムな小文字アルファベットを生成
    part1 = ''.join(random.choices(string.ascii_lowercase, k=5))
    part2 = ''.join(random.choices(string.ascii_lowercase, k=5))
    
    # 形式 xxxxx.xxxxx で結合
    username = f"{part1}.{part2}"
    return username

def generate_productname():
    # 5文字のランダムな小文字アルファベットを生成
    part1 = ''.join(random.choices(string.ascii_lowercase, k=3))
    part2 = ''.join(random.choices(string.ascii_lowercase, k=3))
    part3 = ''.join(random.choices(string.ascii_lowercase, k=3))
    
    # 形式 xxx_xxx_xxx で結合
    productname = f"{part1}_{part2}_{part3}"
    return productname

generate_username_udf = udf(generate_username, StringType())
generate_productname_udf = udf(generate_productname, StringType())

# ユーザーデータの生成
def generate_users(num_users=10000):
    return spark.range(1, num_users + 1).withColumnRenamed("id", "user_id")\
                .withColumn("name", generate_username_udf())\
                .withColumn("age", round(rand() * 60 + 18))\
                .withColumn("gender", when(rand() > 0.5, lit("男性")).otherwise(lit("女性")))\
                .withColumn("email", expr("concat(name, '@example.com')"))\
                .withColumn("registration_date", lit(datetime.date(2020, 1, 1)))\
                .withColumn("region", expr("case when rand() < 0.2 then '北海道' when rand() < 0.4 then '東京' when rand() < 0.6 then '大阪' when rand() < 0.8 then '福岡' else '沖縄' end"))

# 商品データの生成
def generate_products(num_products=100):
    return spark.range(1, num_products + 1).withColumnRenamed("id", "product_id")\
                .withColumn("product_name", generate_productname_udf())\
                .withColumn("category", when(rand() > 0.5, lit("食料品")).otherwise(lit("日用品")))\
                .withColumn("subcategory", when(col("category") == "食料品", when(rand() > 0.5, lit("野菜")).otherwise(lit("果物"))).otherwise(when(rand() > 0.5, lit("洗剤")).otherwise(lit("トイレットペーパー"))))\
                .withColumn("price", round(rand() * 1000 + 100, 2))\
                .withColumn("stock_quantity", round(rand() * 100 + 1))\
                .withColumn("cost_price", round(col("price") * 0.7, 2))

users = generate_users()
products = generate_products()


conditions = [
    # 若年層は新鮮な果物に興味があり、より多く購入する傾向がある。
    ((col("age") < 25) & (col("subcategory") == "果物"), 1),
    
    # 25歳から30歳のユーザーは日用品の購入において実用性を重視しやすい。
    ((col("age") >= 25) & (col("age") < 30) & (col("category") == "日用品"), 2),
    
    # 30歳から35歳は健康に気を使い始め、野菜の購入量が増える。
    ((col("age") >= 30) & (col("age") < 35) & (col("subcategory") == "野菜"), 1),
    
    # 女性は特定の日用品に対して特別なニーズがあり、それに関連する商品の購入を増やす可能性がある。
    ((col("gender") == "女性") & (col("category") == "日用品"), 1),
    
    # 東京のユーザーは食生活において多様性を求める傾向があり、食料品の購入量が増える。
    ((col("region") == "東京") & (col("category") == "食料品"), 1),
    
    # 大阪のユーザーは実用的な日用品の購入を好む。
    ((col("region") == "大阪") & (col("category") == "日用品"), 1),
    
    # 福岡のユーザーは新鮮な果物への関心が高い。
    ((col("region") == "福岡") & (col("subcategory") == "果物"), -2),
    
    # 北海道のユーザーは寒い地域特有の生活用品に対する需要が高い。
    ((col("region") == "北海道") & (col("category") == "日用品"), 1),
    
    # 沖縄のユーザーは地元の果物を好む傾向がある。
    ((col("region") == "沖縄") & (col("subcategory") == "果物"), 2),
    
    # 中年層は健康に対する意識が高まり、野菜の消費を増やす。
    ((col("age") >= 35) & (col("age") < 50) & (col("subcategory") == "野菜"), 2),
    
    # 若年層の男性は、スポーツやアウトドア関連の商品に関心が高いと仮定する。
    ((col("age") < 30) & (col("gender") == "男性") & (col("category") == "日用品"), 1),
    
    # 高齢者は、健康関連商品や日用品に対して高い関心を持つ。
    ((col("age") >= 60) & (col("category") == "日用品"), -2),
    
    # 女性は美容と健康に関連する食品に対して高い関心を持つ。
    ((col("gender") == "女性") & (col("category") == "食料品"), 1),
    
    # 東京の若年層はトレンドに敏感であり、新商品を試す傾向がある。
    ((col("region") == "東京") & (col("age") < 30), 1),
    
    # 大阪の中年層は家庭を持つことが多く、食料品の購入量が増える。
    ((col("region") == "大阪") & (col("age") >= 30) & (col("age") < 50) & (col("category") == "食料品"), 2),
    
    # 福岡のユーザーは地域の特産品に対して高い関心を持ち、関連商品の購入を好む。
    ((col("region") == "福岡") & (col("subcategory") == "野菜"), 1),
    
    # 北海道の若年層はアウトドア活動に関心が高いと仮定し、関連商品の購入が増える。
    ((col("region") == "北海道") & (col("age") < 30) & (col("category") == "日用品"), 1),
    
    # 沖縄の高齢者は地元の伝統食に高い関心を持つ。
    ((col("region") == "沖縄") & (col("age") >= 60) & (col("category") == "食料品"), -2),
    
    # 若年層は便利さを求めて日用品を購入する傾向がある。
    ((col("age") < 25) & (col("category") == "日用品"), 1),
    
    # 中年層の男性は、家庭用品に対する責任感から、関連商品の購入量を増やす。
    ((col("age") >= 35) & (col("age") < 50) & (col("gender") == "男性") & (col("category") == "日用品"), 1),
]

# トランザクションデータの生成
def generate_transactions(users, products, num_transactions=1000000):
    transactions = (
        spark.range(1, num_transactions + 1).withColumnRenamed("id", "transaction_id")
        .withColumn("user_id", expr(f"floor(rand() * {users.count()}) + 1"))
        .withColumn("product_id", expr(f"floor(rand() * {products.count()}) + 1"))
        .withColumn("quantity", round(rand() * 5 + 1))
        .withColumn("price", round(rand() * 1000 + 100, 2))
        .withColumn("store_id", round(rand() * 10 + 1))
        .withColumn("random_date", expr("date_add(date('2024-01-01'), -CAST(rand() * 365 AS INTEGER))"))
        .withColumn("month", date_format("random_date", "M").cast("int"))
        .withColumn("is_weekend", dayofweek("random_date").isin([1, 7]))
        .withColumn("transaction_date", 
            when((rand() < 0.1) & ((expr("month") == 8) | (expr("month") == 12)), expr("random_date"))
            .when((rand() < 0.1) & expr("is_weekend"), expr("random_date"))
            .otherwise(expr("date_add(date('2024-01-01'), -CAST(rand() * 365 AS INTEGER))"))
        )
        .drop("random_date", "month", "is_weekend")
    )
    
    # 傾向スコアに基づいてquantityを調整
    # return transactions
    adjusted_transaction = transactions.join(users, "user_id").join(products.select("product_id","category","subcategory"), "product_id")
    for condition, adjustment in conditions:
        adjusted_transaction = adjusted_transaction.withColumn("quantity", when(condition, col("quantity") + adjustment).otherwise(col("quantity")))
    adjusted_transaction = adjusted_transaction.withColumn("quantity", greatest(lit(0), "quantity"))
    return adjusted_transaction.select("transaction_id", "user_id", "product_id", "quantity", "price", "transaction_date", "store_id")

# フィードバックデータの生成
def generate_feedbacks(users, products, num_feedbacks=50000):
    feedbacks = spark.range(1, num_feedbacks + 1).withColumnRenamed("id", "feedback_id")\
                      .withColumn("user_id", expr(f"floor(rand() * {users.count()}) + 1"))\
                      .withColumn("product_id", expr(f"floor(rand() * {products.count()}) + 1"))\
                      .withColumn("rating", round(rand() * 4 + 1))\
                      .withColumn("date", expr("date_add(date('2022-01-01'), -CAST(rand() * 365 AS INTEGER))"))\
                      .withColumn("type", when(rand() > 0.66, lit("商品")).when(rand() > 0.33, lit("サービス")).otherwise(lit("その他")))\
                      .withColumn("comment", expr("concat('Feedback_', feedback_id)"))
    
    # 傾向スコアに基づいてratingを調整
    adjusted_feedbacks = feedbacks.join(users, "user_id").join(products.select("product_id","category","subcategory"), "product_id")
    for condition, adjustment in conditions:
        adjusted_feedbacks = adjusted_feedbacks.withColumn("rating", when(condition, col("rating") + adjustment).otherwise(col("rating")))
    
    
    adjusted_feedbacks = adjusted_feedbacks.withColumn("rating", greatest(lit(0), least(lit(5), "rating")))
    return adjusted_feedbacks.select("feedback_id", "user_id", "product_id", "rating", "date", "type", "comment")

transactions = generate_transactions(users, products)
feedbacks = generate_feedbacks(users, products)

# 結果の表示（データフレームのサイズによっては表示が重くなる可能性があるため、小さなサンプルで表示）
transactions.show(5)
feedbacks.show(5)

# COMMAND ----------

# DBTITLE 1,テーブルの書き込み
users.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("users")
products.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("products")
transactions.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("transactions")
feedbacks.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("feedbacks")

# COMMAND ----------

# DBTITLE 1,テーブルのメタデータ編集
# MAGIC %sql
# MAGIC ALTER TABLE users ALTER COLUMN user_id COMMENT "整数、ユニーク（主キー）";
# MAGIC ALTER TABLE users ALTER COLUMN name COMMENT "文字列";
# MAGIC ALTER TABLE users ALTER COLUMN age COMMENT "整数、0以上";
# MAGIC ALTER TABLE users ALTER COLUMN gender COMMENT "文字列、'男性'、'女性'、'その他'";
# MAGIC ALTER TABLE users ALTER COLUMN email COMMENT "文字列、メールフォーマット";
# MAGIC ALTER TABLE users ALTER COLUMN registration_date COMMENT "日付、YYYY-MM-DDフォーマット";
# MAGIC ALTER TABLE users ALTER COLUMN region COMMENT "文字列、例: '東京'、'大阪'、'北海道'";
# MAGIC COMMENT ON TABLE users IS 'usersテーブルには、オンラインスーパー「ブリックスマート」に登録されているユーザーに関する情報が含まれています。これには、年齢、性別、地域などの人口統計データや連絡先の詳細が含まれます。このデータは、ユーザーのセグメンテーション、ユーザーの嗜好の理解、さまざまなユーザーグループのニーズに合わせたプラットフォームの特徴量の調整に使用できます。また、ユーザーのエンゲージメントを追跡し、マーケティングキャンペーンの成功を測定するのにも役立ちます。';
# MAGIC
# MAGIC ALTER TABLE transactions ALTER COLUMN transaction_id COMMENT "整数、ユニーク（主キー）";
# MAGIC ALTER TABLE transactions ALTER COLUMN user_id COMMENT "整数、`users`テーブルの`user_id`とリンク（外部キー）";
# MAGIC ALTER TABLE transactions ALTER COLUMN transaction_date COMMENT "日付、YYYY-MM-DDフォーマット";
# MAGIC ALTER TABLE transactions ALTER COLUMN product_id COMMENT "整数";
# MAGIC ALTER TABLE transactions ALTER COLUMN quantity COMMENT "整数、1以上";
# MAGIC ALTER TABLE transactions ALTER COLUMN price COMMENT "浮動小数点数、0以上";
# MAGIC ALTER TABLE transactions ALTER COLUMN store_id COMMENT "整数、購買が行われた店舗のID";
# MAGIC COMMENT ON TABLE transactions IS 'transactionsテーブルには、オンラインスーパー「ブリックスマート」のユーザーによる販売取引に関する情報が含まれます。商品ID、数量、価格、取引日、店舗IDなどの詳細が含まれます。このデータは、販売傾向の分析、ユーザー行動の追跡、さまざまな商品の人気の把握に使用できます。また、最も人気のある店舗や商品を特定するのに役立ち、より良い在庫管理やマーケティング戦略を可能にします。';
# MAGIC
# MAGIC ALTER TABLE products ALTER COLUMN product_id COMMENT "整数、ユニーク（主キー）";
# MAGIC ALTER TABLE products ALTER COLUMN product_name COMMENT "文字列";
# MAGIC ALTER TABLE products ALTER COLUMN category COMMENT "文字列、例: '食料品'、'日用品'";
# MAGIC ALTER TABLE products ALTER COLUMN subcategory COMMENT "文字列、例: '野菜'、'洗剤'";
# MAGIC ALTER TABLE products ALTER COLUMN price COMMENT "浮動小数点数、0以上";
# MAGIC ALTER TABLE products ALTER COLUMN stock_quantity COMMENT "整数、在庫数量";
# MAGIC ALTER TABLE products ALTER COLUMN cost_price COMMENT "浮動小数点数、仕入れ価格";
# MAGIC COMMENT ON TABLE products IS 'productsテーブルには、オンラインスーパー「ブリックスマート」で入手可能なさまざまな商品に関する情報が含まれています。商品名、カテゴリー、サブカテゴリー、価格、在庫数、原価などの詳細が含まれます。このデータは、在庫管理、価格分析、製品分類に使用できます。また、製品のパフォーマンスを追跡し、特定のカテゴリーやサブカテゴリーにおける成長の機会を特定するのにも役立ちます。';
# MAGIC
# MAGIC ALTER TABLE feedbacks ALTER COLUMN feedback_id COMMENT "整数、ユニーク（主キー）";
# MAGIC ALTER TABLE feedbacks ALTER COLUMN user_id COMMENT "整数、`users`テーブルの`user_id`とリンク（外部キー）";
# MAGIC ALTER TABLE feedbacks ALTER COLUMN comment COMMENT "文字列";
# MAGIC ALTER TABLE feedbacks ALTER COLUMN date COMMENT "日付、YYYY-MM-DDフォーマット";
# MAGIC ALTER TABLE feedbacks ALTER COLUMN type COMMENT "文字列、'商品'、'サービス'、'その他'";
# MAGIC ALTER TABLE feedbacks ALTER COLUMN rating COMMENT "整数、1から5までの評価";
# MAGIC COMMENT ON TABLE feedbacks IS 'feedbacksテーブルには、オンラインスーパー「ブリックスマート」における様々な商品へのユーザーフィードバックデータが含まれます。これには、ユーザの評価、フィードバックの日付、フィードバックの種類などの詳細が含まれます。このデータは、ユーザーの嗜好を理解し、製品の問題を特定し、時間の経過に伴うユーザー満足度の変化を追跡するために使用することができます。また、報告された問題の頻度や深刻度に基づいて、製品の改善に優先順位をつける際にも役立ちます。';

# COMMAND ----------

# DBTITLE 1,gold_usersテーブルの生成
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_user AS (
# MAGIC   with avg_data as(
# MAGIC   SELECT 
# MAGIC     u.user_id,
# MAGIC     SUM(CASE WHEN p.category = '食料品' THEN t.quantity ELSE 0 END) AS food_quantity,
# MAGIC     SUM(CASE WHEN p.category = '日用品' THEN t.quantity ELSE 0 END) AS daily_quantity,
# MAGIC     SUM(CASE WHEN p.category NOT IN ('食料品', '日用品') THEN t.quantity ELSE 0 END) AS other_quantity,
# MAGIC     AVG(CASE WHEN p.category = '食料品' THEN f.rating ELSE NULL END) AS food_rating,
# MAGIC     AVG(CASE WHEN p.category = '日用品' THEN f.rating ELSE NULL END) AS daily_rating,
# MAGIC     AVG(CASE WHEN p.category NOT IN ('食料品', '日用品') THEN f.rating ELSE NULL END) AS other_rating
# MAGIC   FROM users u
# MAGIC   LEFT JOIN transactions t ON u.user_id = t.user_id
# MAGIC   LEFT JOIN products p ON t.product_id = p.product_id
# MAGIC   LEFT JOIN feedbacks f ON u.user_id = f.user_id
# MAGIC   GROUP BY u.user_id)
# MAGIC   select * from users join avg_data using(user_id)
# MAGIC );

# COMMAND ----------

# DBTITLE 1,gold_usersテーブルのメタデータ編集
# MAGIC %sql
# MAGIC ALTER TABLE gold_user ALTER COLUMN food_quantity COMMENT "浮動小数点数、食料品の合計購買点数";
# MAGIC ALTER TABLE gold_user ALTER COLUMN daily_quantity COMMENT "浮動小数点数、日用品の合計購買点数";
# MAGIC ALTER TABLE gold_user ALTER COLUMN other_quantity COMMENT "浮動小数点数、その他の合計購買点数";
# MAGIC ALTER TABLE gold_user ALTER COLUMN food_rating COMMENT "浮動小数点数、食料品の平均レビュー評価";
# MAGIC ALTER TABLE gold_user ALTER COLUMN daily_rating COMMENT "浮動小数点数、日用品の平均レビュー評価";
# MAGIC ALTER TABLE gold_user ALTER COLUMN other_rating COMMENT "浮動小数点数、その他の平均レビュー評価";
# MAGIC COMMENT ON TABLE gold_user IS '`gold_user`テーブルには、AIを搭載した食品推薦システムに登録したユーザーに関する情報が含まれている。これには、人口統計学的詳細、食品消費習慣、および評価が含まれる。このデータは、ユーザーの嗜好を理解し、食品の消費傾向を追跡し、AIシステムの有効性を評価するために使用することができる。また、システムの潜在的な改善点を特定し、消費パターンと評価に基づいて個々のユーザーに合わせた推薦を行う際にも役立ちます。'

# COMMAND ----------

# DBTITLE 1,PIIタグの追加
# MAGIC %sql
# MAGIC ALTER TABLE users ALTER COLUMN name SET TAGS ('pii_name');
# MAGIC ALTER TABLE users ALTER COLUMN email SET TAGS ('pii_email');
# MAGIC ALTER TABLE gold_user ALTER COLUMN name SET TAGS ('pii_name');
# MAGIC ALTER TABLE gold_user ALTER COLUMN email SET TAGS ('pii_email');

# COMMAND ----------

# DBTITLE 1,PK & FKの追加
# MAGIC %sql
# MAGIC ALTER TABLE users ALTER COLUMN user_id SET NOT NULL;
# MAGIC ALTER TABLE transactions ALTER COLUMN transaction_id SET NOT NULL;
# MAGIC ALTER TABLE products ALTER COLUMN product_id SET NOT NULL;
# MAGIC ALTER TABLE feedbacks ALTER COLUMN feedback_id SET NOT NULL;
# MAGIC ALTER TABLE gold_user ALTER COLUMN user_id SET NOT NULL;

# MAGIC ALTER TABLE users ADD CONSTRAINT users_pk PRIMARY KEY (user_id);
# MAGIC ALTER TABLE transactions ADD CONSTRAINT transactions_pk PRIMARY KEY (transaction_id);
# MAGIC ALTER TABLE products ADD CONSTRAINT products_pk PRIMARY KEY (product_id);
# MAGIC ALTER TABLE feedbacks ADD CONSTRAINT feedbacks_pk PRIMARY KEY (feedback_id);
# MAGIC ALTER TABLE gold_user ADD CONSTRAINT gold_user_pk PRIMARY KEY (user_id);

# MAGIC ALTER TABLE transactions ADD CONSTRAINT transactions_users_fk FOREIGN KEY (user_id) REFERENCES users (user_id) NOT ENFORCED RELY;
# MAGIC ALTER TABLE transactions ADD CONSTRAINT transactions_products_fk FOREIGN KEY (product_id) REFERENCES products (product_id) NOT ENFORCED RELY;
# MAGIC ALTER TABLE feedbacks ADD CONSTRAINT feedbacks_users_fk FOREIGN KEY (user_id) REFERENCES users (user_id) NOT ENFORCED RELY;
# MAGIC ALTER TABLE feedbacks ADD CONSTRAINT feedbacks_products_fk FOREIGN KEY (product_id) REFERENCES products (product_id) NOT ENFORCED RELY;

# COMMAND ----------

# DBTITLE 1,列レベルマスキングの追加
# MAGIC %sql
# MAGIC CREATE FUNCTION mask_email(email STRING) RETURN CASE WHEN is_member('admins') THEN email ELSE '***@example.com' END;
# MAGIC ALTER TABLE users ALTER COLUMN email SET MASK mask_email;
# MAGIC ALTER TABLE gold_user ALTER COLUMN email SET MASK mask_email;
