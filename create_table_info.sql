-- 自動取得される項目：

-- DATABASE, SCHEMA, TABLE_NAME: INFORMATION_SCHEMA.TABLESから取得
-- COLUMN_NUM: サブクエリでカラム数をカウント
-- RECORD_NUM: ROW_COUNTから取得（概算値）
-- CREATION_DATE: テーブル作成日時
-- UPDATE_DATE: 最終更新日時
-- TABLE_COMMENT: テーブルコメント
-- COLUMN_COMMENT_FLAG: 全カラムにコメントがあるか（1/0）
-- COLUMN_COMMENT: 全カラムのコメントをJSON形式で格納
-- 手動で後から追加する項目：

-- OWNER, SUB_OWNER, PUBLISH, SCOPE, APPLICATION_PROJECT, COMMENT: 初期値NULL


-- TABLE_INFOテーブルを作成
CREATE OR REPLACE TABLE DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO (
    TABLE_NAME VARCHAR(255),
    LOCATION VARCHAR(500),
    ACCOUNT VARCHAR(255),
    CLASSIFICATION VARCHAR(255),
    COLUMN_NUM NUMBER,
    RECORD_NUM NUMBER,
    CREATION_DATE TIMESTAMP_NTZ,
    UPDATE_DATE TIMESTAMP_NTZ,
    OWNER VARCHAR(255),
    SUB_OWNER VARCHAR(255),
    TABLE_COMMENT VARCHAR(1000),
    COLUMN_COMMENT VARCHAR, -- JSON形式でカラムコメントを格納
    COLUMN_COMMENT_FLAG NUMBER(1),
    PUBLISH VARCHAR(255),
    SCOPE VARCHAR(255),
    APPLICATION_PROJECT VARCHAR(255),
    COMMENT VARCHAR(1000)
);

-- Snowflakeのメタデータから情報を取得して挿入
-- ACCOUNT_USAGEを使用してアカウント内の全データベースの情報を取得
INSERT INTO DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO (
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
)
SELECT 
    t.TABLE_NAME,
    CONCAT(t.TABLE_CATALOG, '.', t.TABLE_SCHEMA) AS LOCATION,
    'DIESELPJ' AS ACCOUNT,
    'SNOWFLAKE' AS CLASSIFICATION,
    -- カラム数を取得
    (SELECT COUNT(*) 
     FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS c 
     WHERE c.TABLE_CATALOG = t.TABLE_CATALOG 
       AND c.TABLE_SCHEMA = t.TABLE_SCHEMA 
       AND c.TABLE_NAME = t.TABLE_NAME
       AND c.DELETED IS NULL) AS COLUMN_NUM,
    -- レコード数を取得（注: ACCOUNT_USAGEではROW_COUNTは利用できないため、別途取得が必要）
    NULL AS RECORD_NUM,
    t.CREATED AS CREATION_DATE,
    t.LAST_ALTERED AS UPDATE_DATE,
    NULL AS OWNER,
    NULL AS SUB_OWNER,
    -- テーブルコメントを取得
    t.COMMENT AS TABLE_COMMENT,
    -- カラムコメントをJSON形式で取得（手動でJSONを構築）
    (SELECT '[' || LISTAGG(
         '{"column":"' || c.COLUMN_NAME || '","comment":"' || REPLACE(IFNULL(c.COMMENT, ''), '"', '\\"') || '"}',
         ','
     ) || ']'
     FROM (
         SELECT c.COLUMN_NAME, c.COMMENT
         FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS c
         WHERE c.TABLE_CATALOG = t.TABLE_CATALOG
           AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
           AND c.TABLE_NAME = t.TABLE_NAME
           AND c.DELETED IS NULL
         ORDER BY c.ORDINAL_POSITION
     ) c
    ) AS COLUMN_COMMENT,
    -- カラムコメントフラグ（全カラムにコメントがあれば1、なければ0）
    CASE 
        WHEN (SELECT COUNT(*) 
              FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS c 
              WHERE c.TABLE_CATALOG = t.TABLE_CATALOG 
                AND c.TABLE_SCHEMA = t.TABLE_SCHEMA 
                AND c.TABLE_NAME = t.TABLE_NAME 
                AND c.DELETED IS NULL
                AND (c.COMMENT IS NULL OR LENGTH(TRIM(c.COMMENT)) = 0)) = 0 
        THEN 1 
        ELSE 0 
    END AS COLUMN_COMMENT_FLAG,
    NULL AS PUBLISH,
    NULL AS SCOPE,
    NULL AS APPLICATION_PROJECT,
    NULL AS COMMENT
FROM 
    SNOWFLAKE.ACCOUNT_USAGE.TABLES t
WHERE 
    t.TABLE_TYPE = 'BASE TABLE'
    AND t.DELETED IS NULL
    AND t.TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
ORDER BY 
    t.TABLE_CATALOG, t.TABLE_SCHEMA, t.TABLE_NAME;

-- 確認用クエリ
SELECT * FROM DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO LIMIT 10;
SELECT * FROM DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO WHERE TABLE_NAME='BILLING_CLEAN_CSV';

-- ============================================
-- 既存データを更新するクエリ（自動取得項目のみ更新）
-- ============================================
MERGE INTO DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO AS target
USING (
    SELECT 
        t.TABLE_NAME,
        CONCAT(t.TABLE_CATALOG, '.', t.TABLE_SCHEMA) AS LOCATION,
        'DIESELPJ' AS ACCOUNT,
        'SNOWFLAKE' AS CLASSIFICATION,
        -- カラム数を取得
        (SELECT COUNT(*) 
         FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS c 
         WHERE c.TABLE_CATALOG = t.TABLE_CATALOG 
           AND c.TABLE_SCHEMA = t.TABLE_SCHEMA 
           AND c.TABLE_NAME = t.TABLE_NAME
           AND c.DELETED IS NULL) AS COLUMN_NUM,
        NULL AS RECORD_NUM,
        t.CREATED AS CREATION_DATE,
        t.LAST_ALTERED AS UPDATE_DATE,
        -- テーブルコメントを取得
        t.COMMENT AS TABLE_COMMENT,
        -- カラムコメントをJSON形式で取得
        (SELECT '[' || LISTAGG(
             '{"column":"' || c.COLUMN_NAME || '","comment":"' || REPLACE(IFNULL(c.COMMENT, ''), '"', '\\"') || '"}',
             ','
         ) || ']'
         FROM (
             SELECT c.COLUMN_NAME, c.COMMENT
             FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS c
             WHERE c.TABLE_CATALOG = t.TABLE_CATALOG
               AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
               AND c.TABLE_NAME = t.TABLE_NAME
               AND c.DELETED IS NULL
             ORDER BY c.ORDINAL_POSITION
         ) c
        ) AS COLUMN_COMMENT,
        -- カラムコメントフラグ
        CASE 
            WHEN (SELECT COUNT(*) 
                  FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS c 
                  WHERE c.TABLE_CATALOG = t.TABLE_CATALOG 
                    AND c.TABLE_SCHEMA = t.TABLE_SCHEMA 
                    AND c.TABLE_NAME = t.TABLE_NAME 
                    AND c.DELETED IS NULL
                    AND (c.COMMENT IS NULL OR LENGTH(TRIM(c.COMMENT)) = 0)) = 0 
            THEN 1 
            ELSE 0 
        END AS COLUMN_COMMENT_FLAG
    FROM 
        SNOWFLAKE.ACCOUNT_USAGE.TABLES t
    WHERE 
        t.TABLE_TYPE = 'BASE TABLE'
        AND t.DELETED IS NULL
        AND t.TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
) AS source
ON target.TABLE_NAME = source.TABLE_NAME 
   AND target.LOCATION = source.LOCATION
WHEN MATCHED THEN
    UPDATE SET
        -- 自動取得項目のみ更新（手動入力項目は保持）
        target.ACCOUNT = source.ACCOUNT,
        target.CLASSIFICATION = source.CLASSIFICATION,
        target.COLUMN_NUM = source.COLUMN_NUM,
        target.CREATION_DATE = source.CREATION_DATE,
        target.UPDATE_DATE = source.UPDATE_DATE,
        target.TABLE_COMMENT = source.TABLE_COMMENT,
        target.COLUMN_COMMENT = source.COLUMN_COMMENT,
        target.COLUMN_COMMENT_FLAG = source.COLUMN_COMMENT_FLAG
        -- OWNER, SUB_OWNER, PUBLISH, SCOPE, APPLICATION_PROJECT, COMMENTは更新しない
WHEN NOT MATCHED THEN
    INSERT (
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
    )
    VALUES (
        source.TABLE_NAME,
        source.LOCATION,
        source.ACCOUNT,
        source.CLASSIFICATION,
        source.COLUMN_NUM,
        source.RECORD_NUM,
        source.CREATION_DATE,
        source.UPDATE_DATE,
        NULL, -- OWNER
        NULL, -- SUB_OWNER
        source.TABLE_COMMENT,
        source.COLUMN_COMMENT,
        source.COLUMN_COMMENT_FLAG,
        NULL, -- PUBLISH
        NULL, -- SCOPE
        NULL, -- APPLICATION_PROJECT
        NULL  -- COMMENT
    );

-- ============================================
-- 複数データベース対応: すべての対象データベースを更新
-- ============================================
CREATE OR REPLACE PROCEDURE DIESELPJ_GEN.DATA_CATALOG.UPDATE_TABLE_INFO_ALL_DATABASES()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    var total_update_count = 0;
    var total_insert_count = 0;
    var total_error_count = 0;
    var db_results = [];
    
    // 対象データベースのリスト
    var databases = [
        'DIESELPJ_GEN',
        'DIESELPJ_H2INJ',
        'DIESELPJ_INJ',
        'DIESELPJ_MFG',
        'DIESELPJ_PUMP',
        'DIESELPJ_QA',
        'DIESELPJ_RAIL',
        'DIESELPJ_TEST',
        'DIESELPJ_ZANCHI',
        'FOR_PROTO',
        'KF67257_IP16082_SHARE_DIESELPJ',
        'KF67257_IP16082_SHARE_DIESELPJ_2',
        'ROLETEST',
        'SANDSHREW_STATS',
        'SHARING_DN7TEST',
        'SNOWFLAKE'
    ];
    
    // 各データベースに対して処理を実行
    for (var i = 0; i < databases.length; i++) {
        var db = databases[i];
        try {
            var call_sql = `CALL DIESELPJ_GEN.DATA_CATALOG.UPDATE_TABLE_INFO_REALTIME('${db}')`;
            var stmt = snowflake.createStatement({sqlText: call_sql});
            var result = stmt.execute();
            result.next();
            var result_msg = result.getColumnValue(1);
            db_results.push(db + ': ' + result_msg);
            
            // 結果をパース（簡易版）
            var match = result_msg.match(/更新: (\d+)件, 新規挿入: (\d+)件, エラー: (\d+)件/);
            if (match) {
                total_update_count += parseInt(match[1]);
                total_insert_count += parseInt(match[2]);
                total_error_count += parseInt(match[3]);
            }
        } catch (err) {
            db_results.push(db + ': 処理失敗 - ' + err.message);
        }
    }
    
    var summary = '全体: 更新 ' + total_update_count + '件, 新規挿入 ' + total_insert_count + '件, エラー ' + total_error_count + '件\n\n';
    summary += 'データベース別結果:\n' + db_results.join('\n');
    
    return summary;
$$;

-- 実行例
-- CALL DIESELPJ_GEN.DATA_CATALOG.UPDATE_TABLE_INFO_ALL_DATABASES();

-- ============================================
-- レコード数を更新するストアドプロシージャ
-- ============================================
CREATE OR REPLACE PROCEDURE DIESELPJ_GEN.DATA_CATALOG.UPDATE_RECORD_NUM()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    var update_count = 0;
    var error_count = 0;
    
    // TABLE_INFOから全テーブル情報を取得
    var get_tables_sql = `
        SELECT TABLE_NAME, LOCATION
        FROM DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO
        WHERE RECORD_NUM IS NULL OR RECORD_NUM = 0
    `;
    
    var stmt = snowflake.createStatement({sqlText: get_tables_sql});
    var result = stmt.execute();
    
    while (result.next()) {
        var table_name = result.getColumnValue(1);
        var location = result.getColumnValue(2);
        
        // LOCATIONを分解（DATABASE.SCHEMA）
        var parts = location.split('.');
        var db = parts[0];
        var schema = parts[1];
        
        try {
            // 各データベースのINFORMATION_SCHEMAからROW_COUNTを取得
            var get_row_count_sql = `
                SELECT ROW_COUNT
                FROM ${db}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '${schema}'
                  AND TABLE_NAME = '${table_name}'
            `;
            
            var row_stmt = snowflake.createStatement({sqlText: get_row_count_sql});
            var row_result = row_stmt.execute();
            
            if (row_result.next()) {
                var row_count = row_result.getColumnValue(1);
                
                // TABLE_INFOを更新
                var update_sql = `
                    UPDATE DIESELPJ_GEN.DATA_CATALOG.TABLE_INFO
                    SET RECORD_NUM = ${row_count}
                    WHERE TABLE_NAME = '${table_name}'
                      AND LOCATION = '${location}'
                `;
                
                var update_stmt = snowflake.createStatement({sqlText: update_sql});
                update_stmt.execute();
                update_count++;
            }
        } catch (err) {
            error_count++;
            // エラーは無視して続行
        }
    }
    
    return '更新完了: ' + update_count + '件, エラー: ' + error_count + '件';
$$;

-- レコード数更新プロシージャを実行
-- 注意: テーブル数が多い場合は時間がかかります
CALL DIESELPJ_GEN.DATA_CATALOG.UPDATE_RECORD_NUM();

-- ============================================
-- 自動更新タスクの設定（1時間ごとに実行）
-- ============================================

-- タスク1: TABLE_INFOの自動取得項目を更新（全データベース対象）
-- UPDATE_TABLE_INFO_ALL_DATABASESプロシージャで全対象データベースを更新
CREATE OR REPLACE TASK DIESELPJ_GEN.DATA_CATALOG.TASK_UPDATE_TABLE_INFO
    WAREHOUSE = COMPUTE_WH  -- 使用するウェアハウスを指定
    SCHEDULE = 'USING CRON 0 * * * * Asia/Tokyo'  -- 毎時0分に実行
AS
CALL DIESELPJ_GEN.DATA_CATALOG.UPDATE_TABLE_INFO_ALL_DATABASES();

-- タスク2: レコード数を更新（タスク1の後に実行）
CREATE OR REPLACE TASK DIESELPJ_GEN.DATA_CATALOG.TASK_UPDATE_RECORD_NUM
    WAREHOUSE = COMPUTE_WH  -- 使用するウェアハウスを指定
    AFTER DIESELPJ_GEN.DATA_CATALOG.TASK_UPDATE_TABLE_INFO  -- タスク1の後に実行
AS
CALL DIESELPJ_GEN.DATA_CATALOG.UPDATE_RECORD_NUM();

-- タスクを有効化（ルートタスクから順に有効化）
-- 注意: 実行する前に、使用するウェアハウス名を確認・修正してください
ALTER TASK DIESELPJ_GEN.DATA_CATALOG.TASK_UPDATE_RECORD_NUM RESUME;
ALTER TASK DIESELPJ_GEN.DATA_CATALOG.TASK_UPDATE_TABLE_INFO RESUME;

-- ============================================
-- タスク管理用のクエリ
-- ============================================

-- タスクの実行履歴を確認
SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_UPDATE_TABLE_INFO',
    SCHEDULED_TIME_RANGE_START => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC
LIMIT 20;

-- タスクのステータスを確認
SHOW TASKS LIKE 'TASK_UPDATE%' IN SCHEMA DIESELPJ_GEN.DATA_CATALOG;

-- タスクを一時停止
-- ALTER TASK DIESELPJ_GEN.DATA_CATALOG.TASK_UPDATE_TABLE_INFO SUSPEND;
-- ALTER TASK DIESELPJ_GEN.DATA_CATALOG.TASK_UPDATE_RECORD_NUM SUSPEND;

-- タスクを削除
-- DROP TASK IF EXISTS DIESELPJ_GEN.DATA_CATALOG.TASK_UPDATE_RECORD_NUM;
-- DROP TASK IF EXISTS DIESELPJ_GEN.DATA_CATALOG.TASK_UPDATE_TABLE_INFO;
