-- プロシージャを作成してテーブル内の全カラムにAI生成コメントを自動追加
CREATE OR REPLACE PROCEDURE add_ai_comments_to_all_columns()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    var table_name = 'TB_DXEEP_FORWARD_B5_3_POWERTRAIN_HAIZUHANTEI';
    var schema_name = 'SA122_POWERSYS_CDAP_SHARE';
    var catalog_name = 'DIESELPJ_GEN';
    
    // Step 0: テーブルのコメントを取得
    var get_table_comment_sql = `
        SELECT COMMENT
        FROM ${catalog_name}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '${schema_name}'
          AND TABLE_NAME = '${table_name}'
    `;
    
    var table_stmt = snowflake.createStatement({sqlText: get_table_comment_sql});
    var table_result = table_stmt.execute();
    table_result.next();
    var table_comment = table_result.getColumnValue(1) || 'テーブル説明なし';
    
    // Step 1: テーブルの全カラムを取得
    var get_columns_sql = `
        SELECT COLUMN_NAME, DATA_TYPE
        FROM ${catalog_name}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '${schema_name}'
          AND TABLE_NAME = '${table_name}'
        ORDER BY ORDINAL_POSITION
    `;
    
    var stmt = snowflake.createStatement({sqlText: get_columns_sql});
    var columns = stmt.execute();
    
    var results = [];
    var success_count = 0;
    var error_count = 0;
    
    // Step 2: 各カラムに対してコメントを生成・追加
    while (columns.next()) {
        var column_name = columns.getColumnValue(1);
        var data_type = columns.getColumnValue(2);
        
        try {
            // サンプルデータを取得
            var sample_sql = `SELECT TOP 100 ${column_name} FROM ${catalog_name}.${schema_name}.${table_name}`;
            var sample_stmt = snowflake.createStatement({sqlText: sample_sql});
            var sample_result = sample_stmt.execute();
            var samples = [];
            while (sample_result.next()) {
                var val = sample_result.getColumnValue(1);
                if (val !== null) {
                    samples.push(val);
                }
            }
            var sample_data = samples.join(', ');
            
            // Cortex AIでコメントを生成（テーブルとサンプルデータを参照）
            var generate_sql = `
                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                    'mistral-large2',
                    CONCAT(
                        'テーブル名: ${table_name}\\n',
                        'テーブル説明: ${table_comment}\\n',
                        'カラム名: ${column_name}\\n',
                        'データ型: ${data_type}\\n',
                        'サンプルデータ: ${sample_data}\\n\\n',
                        '【参考例】\\n',
                        'qmin: 最小流量。単位:[mm3/sec]\\n\\n',
                        'KOHIN: 子品番。\\n\\n',
                        '上記のテーブル名、テーブル説明、カラム名とサンプルデータを考慮して、このカラムの技術的な説明を日本語で50文字以内で簡潔に生成して。',
                        '説明文のみを出力し、前置きや補足説明は不要。',
                        '数値型の場合のみ単位を記載。'
                    )
                ) AS comment_text
            `;
            
            var gen_stmt = snowflake.createStatement({sqlText: generate_sql});
            var result = gen_stmt.execute();
            result.next();
            var ai_comment = result.getColumnValue(1);
            
            // ALTER TABLE文を構築して実行
            var escaped_comment = ai_comment.replace(/'/g, "''");
            var alter_sql = `ALTER TABLE ${catalog_name}.${schema_name}.${table_name} ` +
                           `ALTER COLUMN ${column_name} COMMENT '${escaped_comment}'`;
            
            var alter_stmt = snowflake.createStatement({sqlText: alter_sql});
            alter_stmt.execute();
            
            results.push(column_name + ': ' + ai_comment);
            success_count++;
            
        } catch (err) {
            results.push(column_name + ': ERROR - ' + err.message);
            error_count++;
        }
    }
    
    return '処理完了: 成功 ' + success_count + '件, エラー ' + error_count + '件\\n\\n' + results.join('\\n');
$$;

-- プロシージャを実行してすべてのカラムにコメントを追加
CALL add_ai_comments_to_all_columns();

-- すべてのカラムのコメントを確認
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    COMMENT
FROM DIESELPJ_GEN.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'SA122_POWERSYS_CDAP_SHARE'
  AND TABLE_NAME = 'TB_DXEEP_FORWARD_B5_3_POWERTRAIN_HAIZUHANTEI'
ORDER BY ORDINAL_POSITION;
