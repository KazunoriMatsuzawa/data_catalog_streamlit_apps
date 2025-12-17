import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

# Snowflakeセッションを取得
session = get_active_session()

st.set_page_config(layout="wide")
st.title("TABLE_INFO編集")

# セッションステートの初期化
if 'refresh' not in st.session_state:
    st.session_state.refresh = 0

# フィルター・検索セクション
st.markdown("### 🔍 フィルター・検索")
col_filter1, col_filter2 = st.columns([1, 2])

with col_filter1:
    # ロケーション一覧を取得
    try:
        locations_query = """
            SELECT DISTINCT LOCATION
            FROM DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO
            ORDER BY LOCATION
        """
        locations_result = session.sql(locations_query).collect()
        location_list = ["すべて"] + [row['LOCATION'] for row in locations_result]
        
        selected_location = st.selectbox(
            "📁 ロケーションでフィルター",
            location_list,
            key="location_filter"
        )
    except Exception as e:
        st.error(f"ロケーション取得エラー: {str(e)}")
        selected_location = "すべて"

with col_filter2:
    search_text = st.text_input(
        "🔎 テーブル名・ロケーションで検索",
        placeholder="テーブル名またはロケーション名を入力...",
        key="search_text"
    )

st.markdown("---")

# TABLE_INFOテーブルからデータを取得
try:
    # WHERE句を構築
    where_clauses = []
    
    if selected_location != "すべて":
        where_clauses.append(f"LOCATION = '{selected_location}'")
    
    if search_text and search_text.strip():
        # テーブル名またはロケーション名で部分一致検索
        search_escaped = search_text.replace("'", "''")
        where_clauses.append(f"(UPPER(TABLE_NAME) LIKE UPPER('%{search_escaped}%') OR UPPER(LOCATION) LIKE UPPER('%{search_escaped}%'))")
    
    where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    query = f"""
        SELECT 
            TABLE_NAME,
            LOCATION,
            ACCOUNT,
            CLASSIFICATION,
            COLUMN_NUM,
            RECORD_NUM,
            CREATION_DATE,
            UPDATE_DATE,
            OWNER,
            SUB_OWNER,
            TABLE_COMMENT,
            COLUMN_COMMENT,
            COLUMN_COMMENT_FLAG,
            PUBLISH,
            SCOPE,
            APPLICATION_PROJECT,
            COMMENT
        FROM DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO
        WHERE {where_clause}
        ORDER BY LOCATION, TABLE_NAME
    """
    
    result = session.sql(query).collect()
    
    if result:
        # DataFrameに変換
        df = pd.DataFrame([row.as_dict() for row in result])
        
        st.markdown("**📋 TABLE_INFO 編集**")
        st.caption(f"表示件数: {len(df)} 件")
        
        # データエディタで表示・編集
        edited_df = st.data_editor(
            df,
            use_container_width=True,
            hide_index=True,
            disabled=[
                "TABLE_NAME",
                "LOCATION",
                "ACCOUNT",
                "CLASSIFICATION",
                "COLUMN_NUM",
                "RECORD_NUM",
                "CREATION_DATE",
                "UPDATE_DATE",
                "TABLE_COMMENT",
                "COLUMN_COMMENT",
                "COLUMN_COMMENT_FLAG"
            ],
            column_config={
                "TABLE_NAME": st.column_config.TextColumn("テーブル名", width="medium"),
                "LOCATION": st.column_config.TextColumn("ロケーション", width="large"),
                "ACCOUNT": st.column_config.TextColumn("アカウント", width="small"),
                "CLASSIFICATION": st.column_config.TextColumn("分類", width="small"),
                "COLUMN_NUM": st.column_config.NumberColumn("カラム数", width="small"),
                "RECORD_NUM": st.column_config.NumberColumn("レコード数", width="small"),
                "CREATION_DATE": st.column_config.DatetimeColumn("作成日", width="medium"),
                "UPDATE_DATE": st.column_config.DatetimeColumn("更新日", width="medium"),
                "OWNER": st.column_config.TextColumn("オーナー", width="medium"),
                "SUB_OWNER": st.column_config.TextColumn("サブオーナー", width="medium"),
                "TABLE_COMMENT": st.column_config.TextColumn("テーブルコメント", width="large"),
                "COLUMN_COMMENT": st.column_config.TextColumn("カラムコメント(JSON)", width="large"),
                "COLUMN_COMMENT_FLAG": st.column_config.NumberColumn("カラムコメントフラグ", width="small"),
                "PUBLISH": st.column_config.TextColumn("公開", width="medium"),
                "SCOPE": st.column_config.TextColumn("スコープ", width="medium"),
                "APPLICATION_PROJECT": st.column_config.TextColumn("関連プロジェクト", width="medium"),
                "COMMENT": st.column_config.TextColumn("備考", width="large")
            },
            key=f"table_info_editor_{st.session_state.refresh}",
            height=600
        )
        
        st.markdown("---")
        
        # 保存ボタン
        col1, col2, col3 = st.columns([1, 1, 4])
        
        with col1:
            if st.button("💾 変更を保存", type="primary", use_container_width=True):
                try:
                    success_count = 0
                    error_count = 0
                    
                    # 変更があった行を更新
                    for idx in range(len(edited_df)):
                        # 編集可能カラムが変更されたかチェック
                        if (df.loc[idx, 'OWNER'] != edited_df.loc[idx, 'OWNER'] or
                            df.loc[idx, 'SUB_OWNER'] != edited_df.loc[idx, 'SUB_OWNER'] or
                            df.loc[idx, 'PUBLISH'] != edited_df.loc[idx, 'PUBLISH'] or
                            df.loc[idx, 'SCOPE'] != edited_df.loc[idx, 'SCOPE'] or
                            df.loc[idx, 'APPLICATION_PROJECT'] != edited_df.loc[idx, 'APPLICATION_PROJECT'] or
                            df.loc[idx, 'COMMENT'] != edited_df.loc[idx, 'COMMENT']):
                            
                            try:
                                table_name = edited_df.loc[idx, 'TABLE_NAME']
                                location = edited_df.loc[idx, 'LOCATION']
                                
                                # NULL値を適切に処理
                                owner = edited_df.loc[idx, 'OWNER'] if pd.notna(edited_df.loc[idx, 'OWNER']) else None
                                sub_owner = edited_df.loc[idx, 'SUB_OWNER'] if pd.notna(edited_df.loc[idx, 'SUB_OWNER']) else None
                                publish = edited_df.loc[idx, 'PUBLISH'] if pd.notna(edited_df.loc[idx, 'PUBLISH']) else None
                                scope = edited_df.loc[idx, 'SCOPE'] if pd.notna(edited_df.loc[idx, 'SCOPE']) else None
                                app_project = edited_df.loc[idx, 'APPLICATION_PROJECT'] if pd.notna(edited_df.loc[idx, 'APPLICATION_PROJECT']) else None
                                comment = edited_df.loc[idx, 'COMMENT'] if pd.notna(edited_df.loc[idx, 'COMMENT']) else None
                                
                                # エスケープ処理
                                owner_sql = f"'{owner.replace(chr(39), chr(39)+chr(39))}'" if owner else "NULL"
                                sub_owner_sql = f"'{sub_owner.replace(chr(39), chr(39)+chr(39))}'" if sub_owner else "NULL"
                                publish_sql = f"'{publish.replace(chr(39), chr(39)+chr(39))}'" if publish else "NULL"
                                scope_sql = f"'{scope.replace(chr(39), chr(39)+chr(39))}'" if scope else "NULL"
                                app_project_sql = f"'{app_project.replace(chr(39), chr(39)+chr(39))}'" if app_project else "NULL"
                                comment_sql = f"'{comment.replace(chr(39), chr(39)+chr(39))}'" if comment else "NULL"
                                
                                update_query = f"""
                                    UPDATE DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO
                                    SET 
                                        OWNER = {owner_sql},
                                        SUB_OWNER = {sub_owner_sql},
                                        PUBLISH = {publish_sql},
                                        SCOPE = {scope_sql},
                                        APPLICATION_PROJECT = {app_project_sql},
                                        COMMENT = {comment_sql}
                                    WHERE TABLE_NAME = '{table_name}'
                                      AND LOCATION = '{location}'
                                """
                                
                                session.sql(update_query).collect()
                                success_count += 1
                            except Exception as e:
                                error_count += 1
                                st.error(f"行 {idx+1} の更新エラー: {str(e)}")
                    
                    if success_count > 0:
                        st.success(f"✅ {success_count}件を更新しました！")
                        st.session_state.refresh += 1
                        st.rerun()
                    elif error_count == 0:
                        st.info("変更がありませんでした")
                    
                except Exception as e:
                    st.error(f"❌ 保存エラー: {str(e)}")
        
        with col2:
            if st.button("🔄 最新データを再読込", use_container_width=True):
                st.session_state.refresh += 1
                st.rerun()
        
        st.markdown("---")
        st.markdown("**編集可能カラム:**")
        st.caption("✏️ OWNER, SUB_OWNER, PUBLISH, SCOPE, APPLICATION_PROJECT, COMMENT")
        st.caption("🔒 その他のカラムは編集できません")
        
    else:
        st.warning("⚠️ データが見つかりませんでした")

except Exception as e:
    st.error(f"❌ データ取得エラー: {str(e)}")

st.markdown("---")
st.caption("Powered by Powertrain DX Team © DENSO Corporation")
