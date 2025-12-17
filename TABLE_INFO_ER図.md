# TABLE_INFO エンティティ図

## データカタログシステム ER図

```mermaid
erDiagram
    TABLE_INFO {
        VARCHAR(255) TABLE_NAME PK "テーブル名"
        VARCHAR(500) LOCATION PK "データベース.スキーマ"
        VARCHAR(255) ACCOUNT "アカウント名"
        VARCHAR(255) CLASSIFICATION "分類"
        NUMBER COLUMN_NUM "カラム数"
        NUMBER RECORD_NUM "レコード数"
        TIMESTAMP_NTZ CREATION_DATE "作成日時"
        TIMESTAMP_NTZ UPDATE_DATE "更新日時"
        VARCHAR(255) OWNER "オーナー"
        VARCHAR(255) SUB_OWNER "サブオーナー"
        VARCHAR(1000) TABLE_COMMENT "テーブルコメント"
        VARCHAR COLUMN_COMMENT "カラムコメント(JSON)"
        NUMBER(1) COLUMN_COMMENT_FLAG "コメント完了フラグ"
        VARCHAR(255) PUBLISH "公開状況"
        VARCHAR(255) SCOPE "スコープ"
        VARCHAR(255) APPLICATION_PROJECT "関連プロジェクト"
        VARCHAR(1000) COMMENT "備考"
    }
```

## カラム詳細説明

### 主キー
- **TABLE_NAME**: テーブル名
- **LOCATION**: データベース名.スキーマ名 (例: DIESELPJ_GEN.DATA_CATALOG)

### 自動取得項目 (ACCOUNT_USAGE / INFORMATION_SCHEMAから)
| カラム名 | データ型 | 説明 | 取得元 |
|---------|---------|------|--------|
| ACCOUNT | VARCHAR(255) | Snowflake環境名 | 固定値 (DIESELPJ) |
| CLASSIFICATION | VARCHAR(255) | DB基盤名 | 固定値 (SNOWFLAKE) |
| COLUMN_NUM | NUMBER | カラム数 | ACCOUNT_USAGE.COLUMNS |
| RECORD_NUM | NUMBER | レコード数 | INFORMATION_SCHEMA.TABLES |
| CREATION_DATE | TIMESTAMP_NTZ | テーブル作成日時 | ACCOUNT_USAGE.TABLES |
| UPDATE_DATE | TIMESTAMP_NTZ | 最終更新日時 | ACCOUNT_USAGE.TABLES |
| TABLE_COMMENT | VARCHAR(1000) | テーブルコメント | ACCOUNT_USAGE.TABLES |
| COLUMN_COMMENT | VARCHAR | カラムコメント(JSON配列) | ACCOUNT_USAGE.COLUMNS |
| COLUMN_COMMENT_FLAG | NUMBER(1) | 全カラムコメント有無 (1=完了, 0=未完了) | 計算値 |

### 手動入力項目 (NULL初期値)
| カラム名 | データ型 | 説明 | 用途 |
|---------|---------|------|------|
| OWNER | VARCHAR(255) | オーナー | 責任者名 |
| SUB_OWNER | VARCHAR(255) | サブオーナー | 副責任者名 |
| PUBLISH | VARCHAR(255) | 公開状況 | 公開/非公開など |
| SCOPE | VARCHAR(255) | スコープ | 利用範囲・用途 |
| APPLICATION_PROJECT | VARCHAR(255) | 関連プロジェクト | プロジェクト名 |
| COMMENT | VARCHAR(1000) | 備考 | 自由記述欄 |

## COLUMN_COMMENT JSON形式

```json
[
  {
    "column": "カラム名1",
    "comment": "カラムの説明1"
  },
  {
    "column": "カラム名2",
    "comment": "カラムの説明2"
  }
]
```

## データフロー図

```mermaid
flowchart TB
    subgraph Users["ユーザー"]
        USER_EDIT["データ管理者<br/>手動入力"]
    end
    
    subgraph Snowflake["Snowflake メタデータ"]
        AU_TABLES["ACCOUNT_USAGE.TABLES"]
        AU_COLUMNS["ACCOUNT_USAGE.COLUMNS"]
        IS_TABLES["各DB.INFORMATION_SCHEMA.TABLES"]
    end
    
    subgraph Processing["データ処理"]
        CREATE_SQL["CREATE TABLE_INFO SQL"]
        UPDATE_PROC["UPDATE_RECORD_NUM プロシージャ"]
    end
    
    subgraph DataCatalog["データカタログ"]
        TABLE_INFO["TABLE_INFO テーブル"]
    end
    
    subgraph ManualData["手動入力項目"]
        MANUAL_INPUT["OWNER / SUB_OWNER<br/>APPLICATION_PROJECT / SCOPE<br/>PUBLISH / COMMENT"]
    end
    
    subgraph Applications["アプリケーション"]
        SEARCH_APP["TABLE_INFO検索アプリ"]
        DISPLAY_APP["テーブル情報表示アプリ"]
        EDIT_APP["コメント生成・TABLE_INFO編集アプリ"]
    end
    
    AU_TABLES -->|テーブルメタデータ<br/>自動取得| CREATE_SQL
    AU_COLUMNS -->|カラムメタデータ<br/>自動取得| CREATE_SQL
    CREATE_SQL -->|INSERT<br/>初期値NULL| TABLE_INFO
    
    IS_TABLES -->|レコード数<br/>自動更新| UPDATE_PROC
    UPDATE_PROC -->|UPDATE RECORD_NUM| TABLE_INFO
    
    USER_EDIT -->|手動入力| MANUAL_INPUT
    MANUAL_INPUT -->|編集アプリ経由| EDIT_APP
    EDIT_APP -->|UPDATE| TABLE_INFO
    
    TABLE_INFO -->|参照| SEARCH_APP
    TABLE_INFO -->|参照| DISPLAY_APP
    TABLE_INFO -->|参照・更新| EDIT_APP
    
    style TABLE_INFO fill:#ff9,stroke:#333,stroke-width:4px
    style Snowflake fill:#e1f5ff
    style Processing fill:#fff3e0
    style DataCatalog fill:#f1f8e9
    style Applications fill:#fce4ec
    style ManualData fill:#ffebee
    style USER_EDIT fill:#ffcdd2
```

## システムアーキテクチャ

```mermaid
graph LR
    subgraph Users["ユーザー"]
        U1["データアナリスト"]
        U2["データエンジニア"]
        U3["プロジェクトメンバー"]
    end
    
    subgraph StreamlitApps["Streamlit アプリ"]
        direction TB
        S1["AI検索<br/>自然言語でテーブル検索"]
        S2["フィルター検索<br/>DB/Schema/Tableで絞り込み"]
        S3["テーブル情報表示<br/>詳細・カラム・データプレビュー"]
        S4["メタデータ編集<br/>Owner/Project/Scope管理"]
        S5["AIコメント生成<br/>Cortex AIで説明文作成"]
    end
    
    subgraph Database["Snowflake データベース"]
        direction TB
        TI["TABLE_INFO<br/>データカタログ"]
        AU["ACCOUNT_USAGE<br/>システムメタデータ"]
        IS["INFORMATION_SCHEMA<br/>各DBメタデータ"]
    end
    
    subgraph AI["Snowflake Cortex AI"]
        MISTRAL["mistral-large2<br/>キーワード抽出・コメント生成"]
    end
    
    U1 --> S1
    U1 --> S2
    U2 --> S4
    U2 --> S5
    U3 --> S3
    
    S1 --> MISTRAL
    S1 --> TI
    S2 --> TI
    S3 --> TI
    S3 --> IS
    S4 --> TI
    S5 --> MISTRAL
    S5 --> IS
    
    AU -.->|初期データ| TI
    IS -.->|レコード数更新| TI
    
    style TI fill:#ffd54f,stroke:#333,stroke-width:3px
    style MISTRAL fill:#81c784,stroke:#333,stroke-width:2px
```

## データ更新フロー

```mermaid
sequenceDiagram
    participant Admin as 管理者
    participant SQL as CREATE SQL
    participant AU as ACCOUNT_USAGE
    participant TI as TABLE_INFO
    participant PROC as UPDATE_RECORD_NUM
    participant IS as INFORMATION_SCHEMA
    
    Admin->>SQL: 初期構築実行
    SQL->>AU: メタデータ取得
    AU-->>SQL: テーブル・カラム情報
    SQL->>TI: データ挿入
    
    Note over TI: RECORD_NUM = NULL
    
    Admin->>PROC: レコード数更新実行
    loop 各テーブル
        PROC->>IS: ROW_COUNT取得
        IS-->>PROC: レコード数
        PROC->>TI: RECORD_NUM更新
    end
    
    PROC-->>Admin: 更新完了通知
    
    Note over TI: データカタログ完成
```

## 検索機能の仕組み

```mermaid
flowchart LR
    subgraph Input["ユーザー入力"]
        Q1["自然言語<br/>(AI検索)"]
        Q2["キーワード<br/>(キーワード検索)"]
        Q3["DB/Schema/Table<br/>(フィルター検索)"]
    end
    
    subgraph AI_Processing["AI処理"]
        CORTEX["Cortex AI<br/>mistral-large2"]
        KW["キーワード抽出"]
    end
    
    subgraph Search["検索処理"]
        LIKE["LIKE検索<br/>UPPER()"]
        FILTER["完全一致<br/>LOCATION/TABLE_NAME"]
    end
    
    subgraph Columns["検索対象カラム"]
        C1["TABLE_NAME"]
        C2["LOCATION"]
        C3["TABLE_COMMENT"]
        C4["COLUMN_COMMENT"]
        C5["APPLICATION_PROJECT"]
        C6["SCOPE"]
        C7["COMMENT"]
    end
    
    Q1 --> CORTEX
    CORTEX --> KW
    KW --> LIKE
    Q2 --> LIKE
    Q3 --> FILTER
    
    LIKE --> C1
    LIKE --> C2
    LIKE --> C3
    LIKE --> C4
    LIKE --> C5
    LIKE --> C6
    LIKE --> C7
    
    FILTER --> C1
    FILTER --> C2
    
    C1 --> RESULT["検索結果"]
    C2 --> RESULT
    C3 --> RESULT
    C4 --> RESULT
    C5 --> RESULT
    C6 --> RESULT
    C7 --> RESULT
    
    style CORTEX fill:#81c784
    style RESULT fill:#ffd54f
```

---

## 備考
- 作成日: 2025-12-11
- データベース: DIESELPJ_GEN.DATA_CATALOG
- 主な用途: データカタログ、テーブル検索、メタデータ管理
- AI機能: Snowflake Cortex AI (mistral-large2) 使用
