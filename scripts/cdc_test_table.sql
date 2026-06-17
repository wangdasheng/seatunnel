-- ============================================
-- 源端(test)建表
-- ============================================
-- DROP TABLE IF EXISTS test.cdc_test;
CREATE TABLE IF NOT EXISTS test.cdc_test (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    email VARCHAR(128),
    age INT DEFAULT 0,
    status TINYINT DEFAULT 1 COMMENT '1:active 0:inactive',
    score DECIMAL(10,2) DEFAULT 0.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- 目标端(wangzhijun)建表(SeaTunnel 会自动创建，这里手动创建用)
-- ============================================
-- CREATE TABLE IF NOT EXISTS wangzhijun.cdc_test (
--     id INT PRIMARY KEY,
--     name VARCHAR(64) NOT NULL,
--     email VARCHAR(128),
--     age INT DEFAULT 0,
--     status TINYINT DEFAULT 1,
--     score DECIMAL(10,2) DEFAULT 0.00,
--     created_at TIMESTAMP,
--     updated_at TIMESTAMP
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
