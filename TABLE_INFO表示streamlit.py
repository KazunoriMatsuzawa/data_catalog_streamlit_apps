import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

# Snowflakeセッションを取得
session = get_active_session()

st.set_page_config(layout="wide")
st.title("テーブル情報表示")

# セッションステートの初期化
if 'refresh' not in st.session_state:
    st.session_state.refresh = 0

# 左右2カラムレイアウト
left_col, right_col = st.columns([1, 3])

# ===== 左側: コントロールパネル =====
with left_col:
    # TABLE_INFOから一意のデータベース・スキーマ・テーブルを取得
    try:
        # LOCATIONを分解してデータベースとスキーマのリストを作成
        locations = session.sql("""
            SELECT DISTINCT LOCATION
            FROM DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO
            ORDER BY LOCATION
        """).collect()
        
        # データベースのリストを作成
        databases = sorted(list(set([row['LOCATION'].split('.')[0] for row in locations])))
        
    except Exception as e:
        st.error(f"データ取得エラー: {str(e)}")
        databases = []
    
    # データベース選択
    selected_db = st.selectbox("データベース", databases, key="db_select") if databases else None
    
    # スキーマ選択
    selected_schema = None
    if selected_db:
        try:
            schemas_query = f"""
                SELECT DISTINCT SUBSTRING(LOCATION, LENGTH('{selected_db}') + 2) AS SCHEMA_NAME
                FROM DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO
                WHERE LOCATION LIKE '{selected_db}.%'
                ORDER BY SCHEMA_NAME
            """
            schemas = session.sql(schemas_query).collect()
            schema_list = [row['SCHEMA_NAME'] for row in schemas]
            selected_schema = st.selectbox("スキーマ", schema_list, key="schema_select") if schema_list else None
        except Exception as e:
            st.error(f"スキーマ取得エラー: {str(e)}")
    
    # テーブル選択
    selected_table = None
    if selected_db and selected_schema:
        try:
            location = f"{selected_db}.{selected_schema}"
            tables_query = f"""
                SELECT DISTINCT TABLE_NAME
                FROM DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO
                WHERE LOCATION = '{location}'
                ORDER BY TABLE_NAME
            """
            tables = session.sql(tables_query).collect()
            table_list = [row['TABLE_NAME'] for row in tables]
            selected_table = st.selectbox("テーブル", table_list, key="table_select") if table_list else None
        except Exception as e:
            st.error(f"テーブル取得エラー: {str(e)}")
    
    # 更新ボタン
    if st.button("🔄 更新", use_container_width=True):
        st.session_state.refresh += 1
        st.rerun()

# ===== 右側: テーブル情報表示 =====
with right_col:
    if selected_db and selected_schema and selected_table:
        st.subheader(f"{selected_table}")
        
        try:
            # TABLE_INFOからテーブル情報を取得
            location = f"{selected_db}.{selected_schema}"
            table_info_query = f"""
                SELECT *
                FROM DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO
                WHERE LOCATION = '{location}'
                  AND TABLE_NAME = '{selected_table}'
            """
            table_info = session.sql(table_info_query).collect()
            
            if table_info:
                info = table_info[0]
                
                # テーブル概要セクション
                st.markdown("---")
                st.markdown("**📊 テーブル概要**")
                
                # 2カラムレイアウトで情報を表示
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown(f"**ロケーション:** {info['LOCATION']}")
                    st.markdown(f"**カラム数:** {info['COLUMN_NUM']}")
                    st.markdown(f"**レコード数:** {info['RECORD_NUM'] if info['RECORD_NUM'] else 'N/A'}")
                    st.markdown(f"**作成日:** {info['CREATION_DATE']}")
                
                with col2:
                    st.markdown(f"**最終更新日:** {info['UPDATE_DATE']}")
                    st.markdown(f"**オーナー:** {info['OWNER'] if info['OWNER'] else '未設定'}")
                    st.markdown(f"**サブオーナー:** {info['SUB_OWNER'] if info['SUB_OWNER'] else '未設定'}")
                    st.markdown(f"**関連プロジェクト:** {info['APPLICATION_PROJECT'] if info['APPLICATION_PROJECT'] else '未設定'}")
                
                # テーブルコメント
                st.markdown("---")
                st.markdown("**💬 テーブルコメント:**")
                if info['TABLE_COMMENT']:
                    st.info(info['TABLE_COMMENT'])
                else:
                    st.caption("コメントなし")
                
                # その他の情報
                col3, col4, col5 = st.columns(3)
                with col3:
                    st.markdown(f"**公開状況:** {info['PUBLISH'] if info['PUBLISH'] else '未設定'}")
                with col4:
                    st.markdown(f"**スコープ:** {info['SCOPE'] if info['SCOPE'] else '未設定'}")
                with col5:
                    comment_flag = "✅ 完了" if info['COLUMN_COMMENT_FLAG'] == 1 else "❌ 未完了"
                    st.markdown(f"**カラムコメント:** {comment_flag}")
                
                # カラムコメント表示（プルダウン）
                st.markdown("---")
                with st.expander("📋 カラムコメント", expanded=False):
                    try:
                        # INFORMATION_SCHEMAからカラム情報を取得
                        columns_query = f"""
                            SELECT COLUMN_NAME, DATA_TYPE, COMMENT
                            FROM "{selected_db}".INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_SCHEMA = '{selected_schema}'
                              AND TABLE_NAME = '{selected_table}'
                            ORDER BY ORDINAL_POSITION
                        """
                        columns_info = session.sql(columns_query).collect()
                        
                        if columns_info:
                            # データフレームで表示
                            columns_df = pd.DataFrame([
                                {
                                    'カラム名': row['COLUMN_NAME'],
                                    'データ型': row['DATA_TYPE'],
                                    'コメント': row['COMMENT'] if row['COMMENT'] else ""
                                }
                                for row in columns_info
                            ])
                            
                            st.dataframe(
                                columns_df,
                                use_container_width=True,
                                hide_index=True
                            )
                        else:
                            st.caption("カラム情報が取得できませんでした")
                    except Exception as e:
                        st.error(f"カラム情報取得エラー: {str(e)}")
                
                # リネージ表示（プルダウン）
                st.markdown("---")
                with st.expander("🔗 リネージ", expanded=False):
                    st.caption("リネージ情報は現在準備中です")
                    # 将来的にリネージ情報を表示
                
                # データプレビュー（プルダウン）
                st.markdown("---")
                with st.expander("📊 データを表示 (LIMIT 100)", expanded=False):
                    try:
                        # テーブルデータを取得
                        data_query = f"""
                            SELECT *
                            FROM "{selected_db}"."{selected_schema}"."{selected_table}"
                            LIMIT 100
                        """
                        
                        with st.spinner("データ取得中..."):
                            data_result = session.sql(data_query).collect()
                            
                            if data_result:
                                # DataFrameに変換
                                data_df = pd.DataFrame([row.as_dict() for row in data_result])
                                
                                # データ情報を表示
                                st.caption(f"取得件数: {len(data_df)} 件 / カラム数: {len(data_df.columns)} 列")
                                
                                # データフレームを表示
                                st.dataframe(
                                    data_df,
                                    use_container_width=True,
                                    hide_index=False,
                                    height=400
                                )
                                
                                # CSVダウンロードボタン
                                csv = data_df.to_csv(index=False).encode('utf-8-sig')
                                st.download_button(
                                    label="📥 CSVダウンロード",
                                    data=csv,
                                    file_name=f"{selected_table}_preview.csv",
                                    mime="text/csv"
                                )
                            else:
                                st.caption("データがありません")
                    except Exception as e:
                        st.error(f"データ取得エラー: {str(e)}")
                
                # Power BI接続情報
                st.markdown("---")
                st.markdown("**📊 Power BI接続**")
                
                # 接続情報を取得（実際の環境に合わせて調整が必要）
                try:
                    # Snowflakeアカウント情報を取得
                    account_info = session.sql("SELECT CURRENT_ACCOUNT() AS ACCOUNT, CURRENT_REGION() AS REGION").collect()
                    if account_info:
                        account = account_info[0]['ACCOUNT']
                        region = account_info[0]['REGION']
                        
                        # Power BI接続用の情報
                        server_name = f"{account}.{region}.snowflakecomputing.com"
                        database_name = selected_db
                        warehouse = session.sql("SELECT CURRENT_WAREHOUSE() AS WH").collect()[0]['WH']
                        
                        col_btn1, col_btn2 = st.columns(2)
                        
                        with col_btn1:
                            # 接続文字列をテキストエリアに表示
                            connection_info = f"""サーバー: {server_name}
データベース: {database_name}
スキーマ: {selected_schema}
テーブル: {selected_table}
ウェアハウス: {warehouse}"""
                            
                            st.text_area(
                                "接続情報（Power BIで使用）",
                                value=connection_info,
                                height=150,
                                key="powerbi_connection_info"
                            )
                        
                        with col_btn2:
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
                            full_table_path = f"{selected_db}.{selected_schema}.{selected_table}"
                            st.code(full_table_path, language=None)
                            st.caption("↑ このテーブルパスをコピーして使用")
                
                except Exception as e:
                    st.error(f"接続情報取得エラー: {str(e)}")
                
                # 備考
                if info['COMMENT']:
                    st.markdown("---")
                    st.markdown("**📝 備考:**")
                    st.info(info['COMMENT'])
            
            else:
                st.warning("⚠️ テーブル情報が見つかりませんでした")
        
        except Exception as e:
            st.error(f"❌ エラー: {str(e)}")
    
    else:
        st.info("👈 左側からテーブルを選択してください")

st.markdown("---")
st.caption("Powered by Powertrain DX Team © DENSO Corporation")
