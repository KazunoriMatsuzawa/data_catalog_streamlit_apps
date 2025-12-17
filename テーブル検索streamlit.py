import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

# Snowflakeセッションを取得
session = get_active_session()

st.set_page_config(layout="wide")
st.title("テーブル検索")

# セッションステートの初期化
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

# 説明
st.markdown("""
Snowflake Cortex AIを使って関連するテーブルを検索できます。
用途、目的、データの種類などを自然言語で質問してください。
""")

st.markdown("---")

# セッションステートの初期化
if 'search_results' not in st.session_state:
    st.session_state.search_results = None
if 'search_method' not in st.session_state:
    st.session_state.search_method = ""
if 'selected_table_for_detail' not in st.session_state:
    st.session_state.selected_table_for_detail = None
if 'selected_location_for_detail' not in st.session_state:
    st.session_state.selected_location_for_detail = None

# 左右2カラムレイアウト
left_col, right_col = st.columns([1, 2])

# ===== 左側: 検索入力 =====
with left_col:
    st.subheader("🔍 検索オプション")
    
    # タブで検索方法を分ける
    tab1, tab2, tab3 = st.tabs(["💬 AI検索", "🔤 キーワード検索", "📁 フィルター検索"])
    
    # ===== AI検索タブ =====
    with tab1:
        st.markdown("自然言語で質問してください")
        
        ai_query = st.text_area(
            "何をお探しですか？",
            placeholder="例: CAEに関するテーブルを教えて\n例: GDIの耐久データがあるテーブルは？\n例: 品番層別に使うテーブルを探している",
            height=120,
            key="ai_query"
        )
        
        if st.button("🤖 AI検索を実行", type="primary", use_container_width=True, key="ai_search_btn"):
            if ai_query.strip():
                with st.spinner("AI検索中..."):
                    try:
                        # AI検索: Cortex AIでキーワード抽出
                        ai_prompt = f"""
あなたはSnowflakeのデータカタログ検索アシスタントです。
ユーザーの質問から、TABLE_INFOテーブルを検索するための適切なキーワードを抽出してください。

TABLE_INFOのカラム:
- TABLE_NAME: テーブル名
- LOCATION: データベース.スキーマ
- TABLE_COMMENT: テーブルの説明
- COLUMN_COMMENT: カラムの説明
- APPLICATION_PROJECT: 関連プロジェクト名
- SCOPE: スコープ・用途
- COMMENT: 備考

ユーザーの質問: {ai_query}

以下のキーワード（カンマ区切り）のみを返してください。余計な説明は不要です。
例: 売上,sales,売上管理
"""
                        
                        # Cortex AIでキーワード生成
                        try:
                            ai_result = session.sql(f"""
                                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                                    'mistral-large2',
                                    '{ai_prompt.replace("'", "''")}'
                                ) as keywords
                            """).collect()
                            
                            keywords_text = ai_result[0]['KEYWORDS'] if ai_result else ""
                            keywords = [kw.strip() for kw in keywords_text.split(',') if kw.strip()]
                        except:
                            keywords = [ai_query.strip()]
                        
                        # キーワードでTABLE_INFOを検索
                        search_conditions = []
                        for kw in keywords[:5]:  # 最大5キーワード
                            kw_escaped = kw.replace("'", "''")
                            search_conditions.append(f"""
                                (UPPER(TABLE_NAME) LIKE UPPER('%{kw_escaped}%') OR
                                 UPPER(LOCATION) LIKE UPPER('%{kw_escaped}%') OR
                                 UPPER(TABLE_COMMENT) LIKE UPPER('%{kw_escaped}%') OR
                                 UPPER(COLUMN_COMMENT) LIKE UPPER('%{kw_escaped}%') OR
                                 UPPER(APPLICATION_PROJECT) LIKE UPPER('%{kw_escaped}%') OR
                                 UPPER(SCOPE) LIKE UPPER('%{kw_escaped}%') OR
                                 UPPER(COMMENT) LIKE UPPER('%{kw_escaped}%'))
                            """)
                        
                        search_where = " OR ".join(search_conditions) if search_conditions else "1=0"
                        
                        search_query = f"""
                            SELECT 
                                TABLE_NAME,
                                LOCATION,
                                TABLE_COMMENT,
                                COLUMN_COMMENT,
                                APPLICATION_PROJECT,
                                SCOPE,
                                COMMENT,
                                COLUMN_NUM,
                                RECORD_NUM,
                                OWNER,
                                SUB_OWNER
                            FROM DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO
                            WHERE {search_where}
                            ORDER BY TABLE_NAME
                            LIMIT 50
                        """
                        
                        result = session.sql(search_query).collect()
                        st.session_state.search_results = result
                        st.session_state.search_method = f"AI検索: {ai_query}"
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"❌ AI検索エラー: {str(e)}")
            else:
                st.warning("質問を入力してください")
    
    # ===== キーワード検索タブ =====
    with tab2:
        st.markdown("キーワードで検索")
        
        keyword_query = st.text_input(
            "キーワード",
            placeholder="テーブル名、説明、プロジェクト名など",
            key="keyword_query"
        )
        
        if st.button("🔎 キーワード検索", type="primary", use_container_width=True, key="keyword_search_btn"):
            if keyword_query.strip():
                try:
                    kw_escaped = keyword_query.replace("'", "''")
                    search_query = f"""
                        SELECT 
                            TABLE_NAME,
                            LOCATION,
                            TABLE_COMMENT,
                            COLUMN_COMMENT,
                            APPLICATION_PROJECT,
                            SCOPE,
                            COMMENT,
                            COLUMN_NUM,
                            RECORD_NUM,
                            OWNER,
                            SUB_OWNER
                        FROM DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO
                        WHERE UPPER(TABLE_NAME) LIKE UPPER('%{kw_escaped}%')
                           OR UPPER(LOCATION) LIKE UPPER('%{kw_escaped}%')
                           OR UPPER(TABLE_COMMENT) LIKE UPPER('%{kw_escaped}%')
                           OR UPPER(COLUMN_COMMENT) LIKE UPPER('%{kw_escaped}%')
                           OR UPPER(APPLICATION_PROJECT) LIKE UPPER('%{kw_escaped}%')
                           OR UPPER(SCOPE) LIKE UPPER('%{kw_escaped}%')
                           OR UPPER(COMMENT) LIKE UPPER('%{kw_escaped}%')
                        ORDER BY TABLE_NAME
                        LIMIT 50
                    """
                    
                    result = session.sql(search_query).collect()
                    st.session_state.search_results = result
                    st.session_state.search_method = f"キーワード検索: {keyword_query}"
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ キーワード検索エラー: {str(e)}")
            else:
                st.warning("キーワードを入力してください")
    
    # ===== フィルター検索タブ =====
    with tab3:
        st.markdown("フィルターで絞り込み")
        
        # データベース一覧を取得
        try:
            locations_query = """
                SELECT DISTINCT LOCATION
                FROM DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO
                ORDER BY LOCATION
            """
            locations_result = session.sql(locations_query).collect()
            
            # LOCATIONを分解してデータベースリストを作成
            databases = sorted(list(set([row['LOCATION'].split('.')[0] for row in locations_result])))
            database_list = ["すべて"] + databases
            
            selected_database = st.selectbox(
                "📁 データベース",
                database_list,
                key="database_filter"
            )
        except Exception as e:
            st.error(f"データベース取得エラー: {str(e)}")
            selected_database = "すべて"
        
        # スキーマ一覧を取得（データベースが選択されている場合）
        selected_schema = "すべて"
        if selected_database != "すべて":
            try:
                # 選択されたデータベースに属するスキーマを取得
                schemas = sorted(list(set([
                    row['LOCATION'].split('.')[1] 
                    for row in locations_result 
                    if row['LOCATION'].startswith(f"{selected_database}.")
                ])))
                schema_list = ["すべて"] + schemas
                
                selected_schema = st.selectbox(
                    "📂 スキーマ",
                    schema_list,
                    key="schema_filter"
                )
            except Exception as e:
                st.error(f"スキーマ取得エラー: {str(e)}")
                selected_schema = "すべて"
        
        # テーブル一覧を取得（スキーマが選択されている場合）
        selected_table_filter = "すべて"
        if selected_database != "すべて" and selected_schema != "すべて":
            try:
                location = f"{selected_database}.{selected_schema}"
                tables_query = f"""
                    SELECT DISTINCT TABLE_NAME
                    FROM DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO
                    WHERE LOCATION = '{location}'
                    ORDER BY TABLE_NAME
                """
                tables_result = session.sql(tables_query).collect()
                table_list = ["すべて"] + [row['TABLE_NAME'] for row in tables_result]
                
                selected_table_filter = st.selectbox(
                    "📄 テーブル",
                    table_list,
                    key="table_filter"
                )
            except Exception as e:
                st.error(f"テーブル取得エラー: {str(e)}")
                selected_table_filter = "すべて"
        
        if st.button("🔍 フィルター検索", type="primary", use_container_width=True, key="filter_search_btn"):
            try:
                where_clauses = []
                
                # データベースフィルター
                if selected_database != "すべて":
                    if selected_schema != "すべて":
                        # データベースとスキーマが指定されている
                        location = f"{selected_database}.{selected_schema}"
                        where_clauses.append(f"LOCATION = '{location}'")
                        
                        # テーブルフィルター
                        if selected_table_filter != "すべて":
                            where_clauses.append(f"TABLE_NAME = '{selected_table_filter}'")
                    else:
                        # データベースのみ指定（スキーマは全て）
                        where_clauses.append(f"LOCATION LIKE '{selected_database}.%'")
                
                where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
                
                search_query = f"""
                    SELECT 
                        TABLE_NAME,
                        LOCATION,
                        TABLE_COMMENT,
                        APPLICATION_PROJECT,
                        SCOPE,
                        COMMENT,
                        COLUMN_NUM,
                        RECORD_NUM,
                        OWNER,
                        SUB_OWNER
                    FROM DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO
                    WHERE {where_clause}
                    ORDER BY TABLE_NAME
                    LIMIT 50
                """
                
                result = session.sql(search_query).collect()
                st.session_state.search_results = result
                
                # フィルター説明を生成
                filter_parts = []
                if selected_database != "すべて":
                    filter_parts.append(f"DB: {selected_database}")
                if selected_schema != "すべて":
                    filter_parts.append(f"Schema: {selected_schema}")
                if selected_table_filter != "すべて":
                    filter_parts.append(f"Table: {selected_table_filter}")
                
                filter_desc = ", ".join(filter_parts) if filter_parts else "すべて"
                st.session_state.search_method = f"フィルター検索 ({filter_desc})"
                st.rerun()
                
            except Exception as e:
                st.error(f"❌ フィルター検索エラー: {str(e)}")
    
    st.markdown("---")
    
    # 検索リセットボタン
    if st.button("🗑️ 検索結果をクリア", use_container_width=True):
        st.session_state.search_results = None
        st.session_state.search_method = ""
        st.rerun()

# ===== 右側: 検索結果 =====
with right_col:
    st.subheader("📊 検索結果")
    
    if st.session_state.search_results is not None:
        results = st.session_state.search_results
        
        if results:
            st.success(f"✅ {len(results)}件のテーブルが見つかりました")
            st.caption(f"検索方法: {st.session_state.search_method}")
            
            st.markdown("---")
            
            # 検索結果を表示
            for row in results:
                table_name = row['TABLE_NAME']
                location = row['LOCATION']
                table_comment = row['TABLE_COMMENT'] or "（コメントなし）"
                app_project = row['APPLICATION_PROJECT'] or "-"
                scope = row['SCOPE'] or "-"
                comment = row['COMMENT'] or "-"
                col_num = row['COLUMN_NUM']
                rec_num = row['RECORD_NUM']
                owner = row['OWNER'] or "-"
                sub_owner = row['SUB_OWNER'] or "-"
                
                with st.expander(f"📋 {table_name} ({location})"):
                    col_a, col_b = st.columns(2)
                    
                    with col_a:
                        st.markdown(f"**テーブル説明:**  \n{table_comment}")
                        st.markdown(f"**プロジェクト:** {app_project}")
                        st.markdown(f"**スコープ:** {scope}")
                    
                    with col_b:
                        st.markdown(f"**カラム数:** {col_num}")
                        st.markdown(f"**レコード数:** {rec_num:,}" if rec_num else "**レコード数:** N/A")
                        st.markdown(f"**オーナー:** {owner}")
                        st.markdown(f"**サブオーナー:** {sub_owner}")
                    
                    if comment != "-":
                        st.markdown(f"**備考:** {comment}")
                    
                    st.markdown("---")
                    
                    # テーブル詳細を表示するボタン
                    if st.button(f"📊 詳細を表示", key=f"btn_detail_{table_name}_{location.replace('.', '_')}", use_container_width=True, type="primary"):
                        # セッションステートに保存して画面下部に表示
                        st.session_state.selected_table_for_detail = table_name
                        st.session_state.selected_location_for_detail = location
                        st.rerun()
        else:
            st.warning("⚠️ 該当するテーブルが見つかりませんでした")
            st.info("💡 検索条件を変えて再度検索してみてください")
    else:
        st.info("👈 左側の検索オプションから検索を実行してください")
        st.markdown("""
        **検索方法:**
        - **AI検索**: 自然言語で質問
        - **キーワード検索**: キーワードで全文検索
        - **フィルター検索**: ロケーションとテーブル名で絞り込み
        """)

st.markdown("---")

# ===== テーブル詳細情報表示セクション =====
if st.session_state.selected_table_for_detail and st.session_state.selected_location_for_detail:
    table_name = st.session_state.selected_table_for_detail
    location = st.session_state.selected_location_for_detail
    
    st.markdown("### 📋 テーブル詳細情報")
    
    # クリアボタン
    if st.button("詳細表示をクリア", key="clear_detail"):
        st.session_state.selected_table_for_detail = None
        st.session_state.selected_location_for_detail = None
        st.rerun()
    
    st.markdown("---")
    
    try:
        # TABLE_INFOからテーブル情報を取得
        table_info_query = f"""
            SELECT *
            FROM DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO
            WHERE TABLE_NAME = '{table_name}'
              AND LOCATION = '{location}'
        """
        table_info_result = session.sql(table_info_query).collect()
        
        if table_info_result:
            info = table_info_result[0]
            
            # テーブルタイトル
            st.subheader(f"📊 {table_name}")
            st.caption(f"📁 {location}")
            
            st.markdown("---")
            
            # テーブル概要（2カラム）
            st.markdown("**📋 テーブル概要**")
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("カラム数", f"{info['COLUMN_NUM']:,}")
                st.metric("レコード数", f"{info['RECORD_NUM']:,}" if info['RECORD_NUM'] else "N/A")
                st.write(f"**作成日:** {info['CREATION_DATE']}")
                st.write(f"**更新日:** {info['UPDATE_DATE']}")
            
            with col2:
                st.write(f"**オーナー:** {info['OWNER'] or '-'}")
                st.write(f"**サブオーナー:** {info['SUB_OWNER'] or '-'}")
                st.write(f"**プロジェクト:** {info['APPLICATION_PROJECT'] or '-'}")
                st.write(f"**スコープ:** {info['SCOPE'] or '-'}")
            
            st.markdown("---")
            
            # テーブルコメント
            st.markdown("**💬 テーブルコメント**")
            if info['TABLE_COMMENT']:
                st.info(info['TABLE_COMMENT'])
            else:
                st.caption("（コメントなし）")
            
            if info['COMMENT']:
                st.markdown("**📝 備考**")
                st.write(info['COMMENT'])
            
            st.markdown("---")
            
            # カラム情報
            st.markdown("**📑 カラム情報**")
            
            # LOCATIONからデータベースとスキーマを分解
            db_schema = location.split('.')
            if len(db_schema) == 2:
                db_name = db_schema[0]
                schema_name = db_schema[1]
                
                try:
                    columns_query = f"""
                        SELECT 
                            COLUMN_NAME,
                            DATA_TYPE,
                            COMMENT
                        FROM "{db_name}".INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = '{schema_name}'
                          AND TABLE_NAME = '{table_name}'
                        ORDER BY ORDINAL_POSITION
                    """
                    columns_result = session.sql(columns_query).collect()
                    
                    if columns_result:
                        columns_df = pd.DataFrame([
                            {
                                'カラム名': row['COLUMN_NAME'],
                                'データ型': row['DATA_TYPE'],
                                'コメント': row['COMMENT'] or '-'
                            }
                            for row in columns_result
                        ])
                        
                        st.dataframe(
                            columns_df,
                            use_container_width=True,
                            hide_index=True,
                            height=400
                        )
                    else:
                        st.warning("カラム情報を取得できませんでした")
                        
                except Exception as e:
                    st.error(f"カラム情報取得エラー: {str(e)}")
            
            st.markdown("---")
            
            # データプレビュー
            st.markdown("**👀 データプレビュー (先頭100件)**")
            
            try:
                preview_query = f"""
                    SELECT *
                    FROM "{db_name}"."{schema_name}"."{table_name}"
                    LIMIT 100
                """
                preview_result = session.sql(preview_query).collect()
                
                if preview_result:
                    preview_df = pd.DataFrame([row.as_dict() for row in preview_result])
                    
                    st.dataframe(
                        preview_df,
                        use_container_width=True,
                        hide_index=True,
                        height=400
                    )
                    
                    # CSV ダウンロード
                    csv = preview_df.to_csv(index=False).encode('utf-8-sig')
                    st.download_button(
                        label="📥 プレビューデータをCSVダウンロード",
                        data=csv,
                        file_name=f"{table_name}_preview.csv",
                        mime="text/csv"
                    )
                else:
                    st.info("データがありません")
                    
            except Exception as e:
                st.error(f"データプレビューエラー: {str(e)}")
            
            st.markdown("---")
            
            # Power BI 接続情報
            st.markdown("**📊 Power BI接続**")
            
            try:
                # Snowflakeアカウント情報を取得
                account_info = session.sql("SELECT CURRENT_ACCOUNT() AS ACCOUNT, CURRENT_REGION() AS REGION").collect()
                if account_info:
                    account = account_info[0]['ACCOUNT']
                    region = account_info[0]['REGION']
                    
                    # Power BI接続用の情報
                    server_name = f"{account}.{region}.snowflakecomputing.com"
                    database_name = db_name
                    warehouse_info = session.sql("SELECT CURRENT_WAREHOUSE() AS WH").collect()
                    warehouse = warehouse_info[0]['WH'] if warehouse_info else 'N/A'
                    
                    connection_col1, connection_col2 = st.columns(2)
                    
                    with connection_col1:
                        # 接続文字列をテキストエリアに表示
                        connection_info = f"""サーバー: {server_name}
データベース: {database_name}
スキーマ: {schema_name}
テーブル: {table_name}
ウェアハウス: {warehouse}"""
                        
                        st.text_area(
                            "接続情報（Power BIで使用）",
                            value=connection_info,
                            height=150,
                            key="powerbi_connection_info"
                        )
                    
                    with connection_col2:
                        st.markdown("**Power BI接続手順:**")
                        st.info("⚠️ 事前にODBC設定が必要。[リンクをご参照ください](https://globaldenso.sharepoint.com/sites/jp102749/SitePages/Alluser/snowflake%E3%81%AE%E8%AA%8D%E8%A8%BC%E6%96%B9%E5%BC%8F.aspx)")
                        st.markdown("""                                        
1. Power BI Desktopを起動
2. 「データを取得」→「その他」
3. 「Snowflake」を選択
4. サーバー名とウェアハウスを入力
5. データベースとスキーマを選択
6. 対象テーブルを選択
                        """)
                        
                        # テーブルの完全パスをコピー用に表示
                        full_table_path = f"{database_name}.{schema_name}.{table_name}"
                        st.code(full_table_path, language=None)
                        st.caption("↑ このテーブルパスをコピーして使用")
            
            except Exception as e:
                st.error(f"接続情報取得エラー: {str(e)}")
        
        else:
            st.error("テーブル情報が見つかりませんでした")
            
    except Exception as e:
        st.error(f"エラーが発生しました: {str(e)}")

st.markdown("---")
st.caption("Powered by Powertrain DX Team © DENSO Corporation")
