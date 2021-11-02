// 该模块定义了初始化数据库的方法和公共数据库对象。
// @Author: Haart
// @Created: 2021-10-27
package db

import (
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	db *sql.DB
)

// 初始化数据配置。
// dsn_ 数据库连接字符串。
// 尝试根据指定的连接字符串创建数据库连接并且Ping，如果成功则返回nil，否则返回连接时发生的错误。
func InitDB(dsn_ string) error {
	if db_, err := sql.Open("mysql", dsn_); err != nil {
		return err
	} else {
		// TODO: 以下参数应当改为通过配置文件配置。
		db_.SetMaxOpenConns(100)
		db_.SetMaxIdleConns(90)
		db_.SetConnMaxLifetime(20 * time.Minute)

		if err := db_.Ping(); err != nil {
			return err
		} else {
			db = db_
			return nil
		}
	}
}
