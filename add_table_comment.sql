-- プロシージャを作成してテーブルにAI生成コメントを自動追加
CREATE OR REPLACE PROCEDURE add_ai_comment_to_table()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    var table_name = 'TB_DXEEP_FORWARD_B5_3_POWERTRAIN_HAIZUHANTEI';
    var schema_name = 'SA122_POWERSYS_CDAP_SHARE';
    var catalog_name = 'DIESELPJ_GEN';
    
    // Step 1: テーブルのカラム一覧を取得
    var get_columns_sql = `
        SELECT COLUMN_NAME
        FROM ${catalog_name}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '${schema_name}'
          AND TABLE_NAME = '${table_name}'
        ORDER BY ORDINAL_POSITION
    `;
    
    var stmt = snowflake.createStatement({sqlText: get_columns_sql});
    var columns = stmt.execute();
    
    var column_list = [];
    while (columns.next()) {
        column_list.push(columns.getColumnValue(1));
    }
    
    // Step 2: Cortex AIでテーブルコメントを生成
    var generate_sql = `
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large2',
            CONCAT(
                'テーブル名: ${table_name}\\n',
                'スキーマ: ${schema_name}\\n',
                'カラム一覧: ${column_list.join(', ')}\\n\\n',
                '【参考例】\\n',
                'TB_SALES_SUMMARY: 売上集計テーブル。日次・月次の売上データを格納\\n\\n',
                '上記の参考例を基に、このテーブルの目的と用途を日本語で100文字以内で簡潔に説明して。',
                '説明文のみを出力し、前置きや補足説明は不要。'
            )
        ) AS comment_text
    `;
    
    var gen_stmt = snowflake.createStatement({sqlText: generate_sql});
    var result = gen_stmt.execute();
    result.next();
    var ai_comment = result.getColumnValue(1);
    
    // Step 3: ALTER TABLE文を構築して実行
    var escaped_comment = ai_comment.replace(/'/g, "''");
    var alter_sql = `ALTER TABLE ${catalog_name}.${schema_name}.${table_name} ` +
                   `SET COMMENT = '${escaped_comment}'`;
    
    var alter_stmt = snowflake.createStatement({sqlText: alter_sql});
    alter_stmt.execute();
    
    return 'テーブルコメントを追加しました: ' + ai_comment;
$$;

-- プロシージャを実行してテーブルコメントを追加
CALL add_ai_comment_to_table();

-- テーブルコメントが正しく追加されたか確認
SELECT 
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    COMMENT
FROM DIESELPJ_GEN.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'SA122_POWERSYS_CDAP_SHARE'
  AND TABLE_NAME = 'TB_DXEEP_FORWARD_B5_3_POWERTRAIN_HAIZUHANTEI';
