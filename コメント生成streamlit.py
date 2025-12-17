# ###
# Snowflake Streamlitアプリを作成しました。

# 主な機能:

# ステップ1: DB・スキーマ・テーブルをドロップダウンで選択
# ステップ1': 現在のテーブルコメントとカラムコメントを表示
# ステップ2: テーブルコメント自動生成ボタン（Cortex AI使用）
# ステップ3: 生成されたテーブルコメントを編集・保存
# ステップ4: 全カラムコメント自動生成ボタン（Cortex AI使用）
# ステップ5: 各カラムコメントを個別に編集・一括保存
# 特徴:

# Snowflake上で動作するStreamlitアプリ
# Cortex AIでコメントを自動生成
# ユーザーが自由に編集可能
# リアルタイムでコメントを確認・更新
# エラーハンドリング実装
# このファイルをSnowflakeのStreamlit環境にデプロイして使用してください。
# ###



import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

# Snowflakeセッションを取得
session = get_active_session()

st.set_page_config(layout="wide")
st.title("テーブル・カラムコメント生成")

# セッションステートの初期化
if 'refresh' not in st.session_state:
    st.session_state.refresh = 0
if 'generated_col_comments' not in st.session_state:
    st.session_state.generated_col_comments = None
if 'generated_table_comment' not in st.session_state:
    st.session_state.generated_table_comment = None

# 左右2カラムレイアウト
left_col, right_col = st.columns([1, 3])

# ===== 左側: コントロールパネル =====
with left_col:
    #st.header("⚙️ コントロール")
    
    # データベース選択
    databases = session.sql("SHOW DATABASES").collect()
    db_list = [row['name'] for row in databases]
    selected_db = st.selectbox("データベース", db_list, key="db_select")
    
    # スキーマ選択
    selected_schema = None
    if selected_db:
        schemas = session.sql(f'SHOW SCHEMAS IN DATABASE "{selected_db}"').collect()
        schema_list = [row['name'] for row in schemas]
        selected_schema = st.selectbox("スキーマ", schema_list, key="schema_select")
    
    # テーブル選択
    selected_table = None
    if selected_db and selected_schema:
        tables = session.sql(f'SHOW TABLES IN "{selected_db}"."{selected_schema}"').collect()
        table_list = [row['name'] for row in tables]
        selected_table = st.selectbox("テーブル", table_list, key="table_select")
    
    #st.markdown("---")
    
    # ボタン群
    if selected_db and selected_schema and selected_table:
        #st.subheader("📋 現在の状況")
        
        if st.button("🔄 更新", use_container_width=True):
            st.rerun()
        
        # 最新のコメント状況を取得して表示
        try:
            # テーブルコメント取得
            table_info = session.sql(f"""
                SELECT COMMENT
                FROM "{selected_db}".INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{selected_schema}'
                  AND TABLE_NAME = '{selected_table}'
            """).collect()
            
            table_comment = table_info[0]['COMMENT'] if table_info and table_info[0]['COMMENT'] else ""
            
            # カラムコメント統計取得
            columns_info = session.sql(f"""
                SELECT 
                    COUNT(*) as total_columns,
                    COUNT(COMMENT) as commented_columns
                FROM "{selected_db}".INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{selected_schema}'
                  AND TABLE_NAME = '{selected_table}'
            """).collect()
            
            total_cols = columns_info[0]['TOTAL_COLUMNS']
            commented_cols = columns_info[0]['COMMENTED_COLUMNS']
            comment_rate = (commented_cols / total_cols * 100) if total_cols > 0 else 0
            
            st.metric(
                label="テーブルコメント",
                value="あり" if table_comment else "なし",
                delta=f"{len(table_comment)}文字" if table_comment else None
            )
            
            st.metric(
                label="カラムコメント",
                value=f"{commented_cols}/{total_cols}",
                delta=f"{comment_rate:.0f}%"
            )
            
        except Exception as e:
            st.error(f"取得エラー: {str(e)}")
        
        st.markdown("---")
        #st.subheader("AI生成")
        st.markdown("**AI生成**")
        
        if st.button("テーブルコメント生成", use_container_width=True, type="primary"):
            with st.spinner("生成中..."):
                try:
                    # プロシージャ作成・実行
                    session.sql(f"""
                        CREATE OR REPLACE PROCEDURE gen_tbl_cmt_{selected_table}()
                        RETURNS VARCHAR
                        LANGUAGE JAVASCRIPT
                        AS
                        $$
                            var get_columns_sql = `
                                SELECT COLUMN_NAME
                                FROM "{selected_db}".INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = '{selected_schema}'
                                  AND TABLE_NAME = '{selected_table}'
                                ORDER BY ORDINAL_POSITION
                            `;
                            
                            var stmt = snowflake.createStatement({{sqlText: get_columns_sql}});
                            var columns = stmt.execute();
                            var column_list = [];
                            while (columns.next()) {{
                                column_list.push(columns.getColumnValue(1));
                            }}
                            
                            var generate_sql = `
                                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                                    'mistral-large2',
                                    CONCAT(
                                        'テーブル名: {selected_table}\\\\n',
                                        'カラム一覧: ` + column_list.join(', ') + `\\\\n\\\\n',
                                        '【参考例】\\\\n',
                                        'TB_SALES_SUMMARY: 売上集計テーブル。日次・月次の売上データを格納\\\\n\\\\n',
                                        '上記を参考に、このテーブルの目的を日本語で100文字以内で説明して。説明文のみ出力。'
                                    )
                                ) AS comment_text
                            `;
                            
                            var gen_stmt = snowflake.createStatement({{sqlText: generate_sql}});
                            var result = gen_stmt.execute();
                            result.next();
                            var ai_comment = result.getColumnValue(1);
                            
                            return ai_comment;
                        $$
                    """).collect()
                    
                    result = session.sql(f"CALL gen_tbl_cmt_{selected_table}()").collect()
                    generated_comment = result[0][0] if result else ""
                    
                    # セッションステートに保存
                    st.session_state.generated_table_comment = generated_comment
                    st.success("✅ 生成完了！")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ エラー: {str(e)}")
        
        if st.button("カラムコメント生成", use_container_width=True, type="primary", key="gen_col_comments"):
            with st.spinner("生成中..."):
                try:
                    # プロシージャ作成・実行
                    session.sql(f"""
                        CREATE OR REPLACE PROCEDURE gen_col_cmt_{selected_table}()
                        RETURNS VARCHAR
                        LANGUAGE JAVASCRIPT
                        AS
                        $$
                            var get_table_comment_sql = `
                                SELECT COMMENT
                                FROM "{selected_db}".INFORMATION_SCHEMA.TABLES
                                WHERE TABLE_SCHEMA = '{selected_schema}'
                                  AND TABLE_NAME = '{selected_table}'
                            `;
                            
                            var table_stmt = snowflake.createStatement({{sqlText: get_table_comment_sql}});
                            var table_result = table_stmt.execute();
                            table_result.next();
                            var table_comment = table_result.getColumnValue(1) || 'テーブル説明なし';
                            
                            var get_columns_sql = `
                                SELECT COLUMN_NAME, DATA_TYPE
                                FROM "{selected_db}".INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = '{selected_schema}'
                                  AND TABLE_NAME = '{selected_table}'
                                ORDER BY ORDINAL_POSITION
                            `;
                            
                            var stmt = snowflake.createStatement({{sqlText: get_columns_sql}});
                            var columns = stmt.execute();
                            var result_array = [];
                            
                            while (columns.next()) {{
                                var col_name = columns.getColumnValue(1);
                                var data_type = columns.getColumnValue(2);
                                
                                try {{
                                    // サンプルデータを取得
                                    var sample_sql = `SELECT TOP 100 "` + col_name + `" FROM "{selected_db}"."{selected_schema}"."{selected_table}"`;
                                    var sample_stmt = snowflake.createStatement({{sqlText: sample_sql}});
                                    var sample_result = sample_stmt.execute();
                                    var samples = [];
                                    while (sample_result.next()) {{
                                        var val = sample_result.getColumnValue(1);
                                        if (val !== null) {{
                                            samples.push(val.toString());
                                        }}
                                    }}
                                    var sample_data = samples.join(', ');
                                    
                                    var generate_sql = `
                                        SELECT SNOWFLAKE.CORTEX.COMPLETE(
                                            'mistral-large2',
                                            CONCAT(
                                                'テーブル: {selected_table}\\\\n',
                                                'テーブル説明: ` + table_comment + `\\\\n',
                                                'カラム名: ` + col_name + `\\\\n',
                                                'データ型: ` + data_type + `\\\\n',
                                                'サンプルデータ: ` + sample_data + `\\\\n\\\\n',
                                                '【参考例】\\\\n',
                                                'qmin: 最小流量。単位:[mm3/sec]\\\\n\\\\n',
                                                'KOHIN: 子品番。\\\\n\\\\n',
                                                '上記のテーブル名、テーブル説明、カラム名とサンプルデータを考慮して、このカラムの技術的な説明を日本語で50文字以内で簡潔に生成して。\\\\n',
                                                '説明文のみを出力し、前置きや補足説明は不要。\\\\n',
                                                '数値型の場合のみ単位を記載。'
                                            )
                                        ) AS comment_text
                                    `;
                                    
                                    var gen_stmt = snowflake.createStatement({{sqlText: generate_sql}});
                                    var result = gen_stmt.execute();
                                    result.next();
                                    var ai_comment = result.getColumnValue(1);
                                    
                                    result_array.push(col_name + '|' + ai_comment);
                                }} catch (err) {{
                                    result_array.push(col_name + '|ERROR');
                                }}
                            }}
                            
                            return result_array.join('^^');
                        $$
                    """).collect()
                    
                    result = session.sql(f"CALL gen_col_cmt_{selected_table}()").collect()
                    result_str = result[0][0]
                    
                    # 結果をパース
                    result_list = result_str.split('^^')
                    generated_comments = {}
                    success_count = 0
                    for item in result_list:
                        if '|' in item and 'ERROR' not in item:
                            col_name, comment = item.split('|', 1)
                            generated_comments[col_name] = comment
                            success_count += 1
                    
                    # セッションステートに保存
                    st.session_state.generated_col_comments = generated_comments
                    st.success(f"✅ {success_count}件のコメントが生成されました")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ エラー: {str(e)}")

# ===== 右側: コメント表示・編集 =====
with right_col:
    if selected_db and selected_schema and selected_table:
        st.subheader(f"{selected_table}")
        
        # 最新のテーブルコメント取得
        table_info = session.sql(f"""
            SELECT COMMENT
            FROM "{selected_db}".INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{selected_schema}'
              AND TABLE_NAME = '{selected_table}'
        """).collect()
        
        current_table_comment = table_info[0]['COMMENT'] if table_info and table_info[0]['COMMENT'] else ""
        
        # 最新のカラムコメント取得
        columns_info = session.sql(f"""
            SELECT COLUMN_NAME, DATA_TYPE, COMMENT
            FROM "{selected_db}".INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{selected_schema}'
              AND TABLE_NAME = '{selected_table}'
            ORDER BY ORDINAL_POSITION
        """).collect()
        
        # 現在のコメントを表示・編集
        #st.subheader("📖 コメントを表示・編集")
        st.markdown("---")
        st.markdown("**テーブルコメント:**")
        
        # 生成されたテーブルコメントがある場合
        if st.session_state.generated_table_comment:
            st.info("💡 生成されたコメントです。編集後に「保存」ボタンを押してください。")
            
            edited_generated_table_comment = st.text_area(
                "生成されたテーブルコメント",
                value=st.session_state.generated_table_comment if st.session_state.generated_table_comment else "",
                height=80,
                key=f"generated_table_comment_{selected_table}_{st.session_state.refresh}",
                label_visibility="collapsed"
            )
            
            col_tbl_gen1, col_tbl_gen2 = st.columns(2)
            with col_tbl_gen1:
                if st.button("💾 生成コメント保存", key="save_generated_table_comment", use_container_width=True, type="primary"):
                    try:
                        escaped = edited_generated_table_comment.replace("'", "''")
                        session.sql(f"""
                            ALTER TABLE "{selected_db}"."{selected_schema}"."{selected_table}"
                            SET COMMENT = '{escaped}'
                        """).collect()
                        st.success("✅ 保存しました！")
                        st.session_state.generated_table_comment = None
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ エラー: {str(e)}")
            
            with col_tbl_gen2:
                if st.button("❌ キャンセル", key="cancel_generated_table_comment", use_container_width=True):
                    st.session_state.generated_table_comment = None
                    st.rerun()
        else:
            # 通常の編集フロー
            # デバッグ用: コメントの有無を確認
            if current_table_comment:
                st.caption(f"💬 現在のコメント長: {len(current_table_comment)}文字")
            else:
                st.caption("💬 コメントなし")
            
            edited_table_comment_quick = st.text_area(
                "テーブルコメント",
                value=current_table_comment if current_table_comment else "",
                height=80,
                key=f"quick_table_comment_{selected_table}_{st.session_state.refresh}",
                label_visibility="collapsed"
            )
            
            if st.button("💾 テーブルコメント保存", key="save_table_quick"):
                try:
                    escaped = edited_table_comment_quick.replace("'", "''")
                    session.sql(f"""
                        ALTER TABLE "{selected_db}"."{selected_schema}"."{selected_table}"
                        SET COMMENT = '{escaped}'
                    """).collect()
                    st.success("✅ 保存しました！")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ エラー: {str(e)}")
        
        st.markdown("---")
        st.markdown("**カラムコメント:**")
        
        # 生成されたコメントがある場合は、それを表示・編集
        if st.session_state.generated_col_comments:
            st.info("💡 生成されたコメントです。編集後に「保存」ボタンを押してください。")
            
            # 生成コメントをDataFrameに変換
            generated_df = pd.DataFrame([
                {
                    'カラム名': col_name,
                    'データ型': next((row['DATA_TYPE'] for row in columns_info if row['COLUMN_NAME'] == col_name), ''),
                    'コメント': generated_comments[col_name]
                }
                for col_name, generated_comments in [(k, st.session_state.generated_col_comments) for k in st.session_state.generated_col_comments.keys()]
            ])
            
            generated_edited_df = st.data_editor(
                generated_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "カラム名": st.column_config.TextColumn("カラム名", disabled=True, width="medium"),
                    "データ型": st.column_config.TextColumn("データ型", disabled=True, width="medium"),
                    "コメント": st.column_config.TextColumn("コメント", width="large")
                },
                key=f"generated_comment_editor_{selected_table}_{st.session_state.refresh}"
            )
            
            col_gen_btn1, col_gen_btn2 = st.columns([1, 3])
            with col_gen_btn1:
                if st.button("💾 生成コメント保存", key="save_generated_comments", use_container_width=True, type="primary"):
                    try:
                        success_count = 0
                        for idx, row in generated_edited_df.iterrows():
                            col_name = row['カラム名']
                            comment = row['コメント'] if pd.notna(row['コメント']) else ""
                            escaped = comment.replace("'", "''")
                            session.sql(f"""
                                ALTER TABLE "{selected_db}"."{selected_schema}"."{selected_table}"
                                ALTER COLUMN "{col_name}" COMMENT '{escaped}'
                            """).collect()
                            success_count += 1
                        
                        st.success(f"✅ {success_count}件保存しました！")
                        st.session_state.generated_col_comments = None
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ エラー: {str(e)}")
            
            with col_gen_btn2:
                if st.button("❌ キャンセル", key="cancel_generated_comments", use_container_width=True):
                    st.session_state.generated_col_comments = None
                    st.rerun()
        else:
            # 通常の編集フロー
            # 編集可能なデータフレーム
            comment_df = pd.DataFrame([
                {
                    'カラム名': row['COLUMN_NAME'],
                    'データ型': row['DATA_TYPE'],
                    'コメント': row['COMMENT'] if row['COMMENT'] else ""
                }
                for row in columns_info
            ])
            
            edited_df = st.data_editor(
                comment_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "カラム名": st.column_config.TextColumn("カラム名", disabled=True, width="medium"),
                    "データ型": st.column_config.TextColumn("データ型", disabled=True, width="medium"),
                    "コメント": st.column_config.TextColumn("コメント", width="large")
                },
                key=f"comment_editor_{selected_table}_{st.session_state.refresh}"
            )
            
            col_btn1, col_btn2 = st.columns([1, 3])
            with col_btn1:
                if st.button("💾 カラムコメント保存", key="save_columns_quick", use_container_width=True):
                    try:
                        success_count = 0
                        for idx, row in edited_df.iterrows():
                            col_name = row['カラム名']
                            comment = row['コメント'] if pd.notna(row['コメント']) else ""
                            escaped = comment.replace("'", "''")
                            session.sql(f"""
                                ALTER TABLE "{selected_db}"."{selected_schema}"."{selected_table}"
                                ALTER COLUMN "{col_name}" COMMENT '{escaped}'
                            """).collect()
                            success_count += 1
                        
                        st.success(f"✅ {success_count}件保存しました！")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ エラー: {str(e)}")
        
        # TABLE_INFO編集セクション
        st.markdown("---")
        st.markdown("**📋 TABLE_INFO メタデータ編集:**")
        
        try:
            # TABLE_INFOから該当テーブルの情報を取得
            location = f"{selected_db}.{selected_schema}"
            table_info_query = f"""
                SELECT 
                    OWNER,
                    SUB_OWNER,
                    PUBLISH,
                    SCOPE,
                    APPLICATION_PROJECT,
                    COMMENT
                FROM DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO
                WHERE TABLE_NAME = '{selected_table}'
                  AND LOCATION = '{location}'
            """
            table_info_result = session.sql(table_info_query).collect()
            
            if table_info_result:
                info = table_info_result[0]
                
                # 編集フォーム
                with st.form(key=f"table_info_form_{selected_table}_{st.session_state.refresh}"):
                    col_meta1, col_meta2 = st.columns(2)
                    
                    with col_meta1:
                        owner = st.text_input(
                            "オーナー", 
                            value=info['OWNER'] if info['OWNER'] else "",
                            placeholder="aaa@jp.denso.com"
                        )
                        sub_owner = st.text_input(
                            "サブオーナー（オプション）", 
                            value=info['SUB_OWNER'] if info['SUB_OWNER'] else "",
                            placeholder="オプション"
                        )
                        publish = st.text_input(
                            "公開状況", 
                            value=info['PUBLISH'] if info['PUBLISH'] else "",
                            placeholder="公開 or 非公開"
                        )
                    
                    with col_meta2:
                        scope = st.text_input(
                            "公開範囲（オプション）", 
                            value=info['SCOPE'] if info['SCOPE'] else "",
                            placeholder="オプション"
                        )
                        app_project = st.text_input(
                            "関連プロジェクト（オプション）", 
                            value=info['APPLICATION_PROJECT'] if info['APPLICATION_PROJECT'] else "",
                            placeholder="オプション"
                        )
                    
                    comment = st.text_area(
                        "備考（自由記入欄・オプション）", 
                        value=info['COMMENT'] if info['COMMENT'] else "", 
                        height=100,
                        placeholder="オプション"
                    )
                    
                    submitted = st.form_submit_button("💾 TABLE_INFO 保存", use_container_width=True, type="primary")
                    
                    if submitted:
                        try:
                            # エスケープ処理
                            owner_escaped = owner.replace("'", "''")
                            sub_owner_escaped = sub_owner.replace("'", "''")
                            publish_escaped = publish.replace("'", "''")
                            scope_escaped = scope.replace("'", "''")
                            app_project_escaped = app_project.replace("'", "''")
                            comment_escaped = comment.replace("'", "''")
                            
                            # UPDATE文実行
                            update_sql = f"""
                                UPDATE DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO
                                SET 
                                    OWNER = '{owner_escaped}',
                                    SUB_OWNER = '{sub_owner_escaped}',
                                    PUBLISH = '{publish_escaped}',
                                    SCOPE = '{scope_escaped}',
                                    APPLICATION_PROJECT = '{app_project_escaped}',
                                    COMMENT = '{comment_escaped}'
                                WHERE TABLE_NAME = '{selected_table}'
                                  AND LOCATION = '{location}'
                            """
                            session.sql(update_sql).collect()
                            st.success("✅ TABLE_INFO を更新しました！")
                            st.session_state.refresh += 1
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ エラー: {str(e)}")
            else:
                st.info("📝 このテーブルはTABLE_INFOに登録されていません")
                
        except Exception as e:
            st.error(f"❌ TABLE_INFO取得エラー: {str(e)}")
    
    else:
        st.info("👈 左側からテーブルを選択してください")

st.markdown("---")
st.caption("Powered by Powertrain DX Team © DENSO Corporation")
