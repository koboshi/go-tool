package tool

import (
	"database/sql"
	"github.com/go-sql-driver/mysql"
	"fmt"
	"strings"
	"time"
)

// 数据库
type Database struct {
	db *sql.DB
}

// 根据传入参数创建数据库链接
// 返回sql.DB以及error
func (database *Database) Connect (host string, username string, password string, dbname string, charset string, customParams map[string]string) error {
	//DSN: [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	//创建data srouce name
	params := make(map[string]string)
	params["charset"] = charset
	params["parseTime"] = "true"
	params["readTimeout"] = "30m"
	params["writeTimeout"] = "1m"
	params["allowNativePasswords"] = "true"
	for param, value := range customParams {
		params[param] = value
	}

	config := new(mysql.Config)
	config.Addr = host
	config.User = username
	config.Passwd = password
	config.DBName = dbname
	config.Params = params

	var err error
	database.db, err = sql.Open("mysql", config.FormatDSN())
	if err != nil {
		panic(err)
	}
	return err
}

// 执行查询，返回多行数据集
func (database *Database) Query (sql string, args ...interface{}) (*sql.Rows, error) {
	rows, err := database.db.Query(sql, args...)
	if err != nil {
		panic(err)
	}
	return rows, err
}

// 执行查询，返回单个数据集
func (database *Database) QueryOne(sql string, args ...interface{}) (*sql.Row) {
	return database.db.QueryRow(sql, args...)
}

func (database *Database) internalInsert (data map[string]interface{}, tblName string, insertType string) (int64, error) {
	fields := make([]string, 0, 10)
	values := make([]interface{}, 0, 10)
	var subField string
	for field, value := range data {
		subField = fmt.Sprintf("`%s` = ?", field)
		fields = append(fields, subField)
		values = append(values, value)
	}
	setStr := strings.Join(fields, ", ")
	insertSql := fmt.Sprintf("%s INTO %s SET %s", insertType, tblName, setStr)
	result, err := database.db.Exec(insertSql, values...)
	if err != nil {
		panic(err)
	}
	var lastInsertId int64
	lastInsertId, _ = result.LastInsertId()
	return lastInsertId, err
}

// 新增数据至mysql
// 要新增的数据以map形式传入
// 返回sql.Result以及error
func (database *Database) Insert(data map[string]interface{}, tblName string) (int64, error) {
	return database.internalInsert(data, tblName, "INSERT")
}

// 以INSERT IGNORE INTO形式新增数据至mysql
// 要新增的数据以map形式传入
// 返回sql.Result以及error
func (database *Database) Ignore(data map[string]interface{}, tblName string) (int64, error) {
	return database.internalInsert(data, tblName, "INSERT IGNORE")
}

// 以REPLACE INTO形式新增数据至mysql
// 要新增的数据以map形式传入
// 返回sql.Result以及error
func (database *Database) Replace(data map[string]interface{}, tblName string) (int64, error) {
	return database.internalInsert(data, tblName, "REPLACE")
}

// 更新mysql数据
// 要更新的数据以map形式传入
// UPDATE的WHERE语句以字符串形式传入，支持传入where语句参数，占位符为 ? ,会自动转义
// 返回sql.Result以及error
func (database *Database) Update(data map[string]interface{}, tblName string, whereStr string, whereArgs ...interface{}) (int64, error) {
	fields := make([]string, 0, 10)
	values := make([]interface{}, 0, 10)
	for field, value := range data {
		var subField = fmt.Sprintf("`%s` = ?", field)
		fields = append(fields, subField)
		values = append(values, value)
	}
	setStr := strings.Join(fields, ", ")
	updateSql := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tblName, setStr, whereStr)
	if len(whereArgs) > 0 {
		values = append(values, whereArgs...)
	}
	result, err := database.db.Exec(updateSql, values...)
	if err != nil {
		panic(err)
	}
	var affectedRows int64
	affectedRows, _ = result.RowsAffected()
	return affectedRows, err
}

// 删除mysql数据
// DELETE FROM的WHERE语句以字符串形式传入，支持传入where语句参数，占位符为 ? ,会自动转义
// 返回sql.Result以及error
func (database *Database) Delete(tblName string, whereStr string, whereArgs ...interface{}) (int64, error) {
	deleteSql := fmt.Sprintf("DELETE FROM %s WHERE %s", tblName, whereStr)
	result, err := database.db.Exec(deleteSql, whereArgs...)
	if err != nil {
		panic(err)
	}
	var affectedRows int64
	affectedRows, _ = result.RowsAffected()
	return affectedRows, err
}

// 关闭数据库链接
func (database *Database) Close() (error) {
	return database.db.Close()
}

//开始事务，返回DatabaseTx
func (database *Database) Begin() (*DatabaseTx, error) {
	databaseTx := new(DatabaseTx)
	var err error
	databaseTx.tx, err = database.db.Begin()
	if err != nil {
		panic(err)
	}
	return databaseTx, err
}

// 设置连接池配置参数
// 最大打开链接数
// 最大空闲链接
// 链接重用次数 <=0永久重用
func (database *Database) SetPool(maxOpenConns int, maxIdleConns int, connMaxLifetime time.Duration) {
	database.db.SetMaxOpenConns(maxOpenConns)
	database.db.SetMaxIdleConns(maxIdleConns)
	database.db.SetConnMaxLifetime(connMaxLifetime)
}

// 数据库事务
type DatabaseTx struct {
	tx *sql.Tx
}

func (databaseTx *DatabaseTx) Commit() (error) {
	return databaseTx.tx.Commit()
}

func (databaseTx *DatabaseTx) Rollback() (error) {
	return databaseTx.tx.Rollback()
}

// 执行查询，返回多行数据集
func (databaseTx *DatabaseTx) Query (sql string, args ...interface{}) (*sql.Rows, error) {
	rows, err := databaseTx.tx.Query(sql, args...)
	if err != nil {
		panic(err)
	}
	return rows, err
}

// 执行查询，返回单个数据集
func (databaseTx *DatabaseTx) QueryOne(sql string, args ...interface{}) (*sql.Row) {
	return databaseTx.tx.QueryRow(sql, args...)
}

func (databaseTx *DatabaseTx) internalInsert( data map[string]interface{}, tblName string, insertType string) (int64, error) {
	fields := make([]string, 0, 10)
	values := make([]interface{}, 0, 10)
	var subField string
	for field, value := range data {
		subField = fmt.Sprintf("`%s` = ?", field)
		fields = append(fields, subField)
		values = append(values, value)
	}
	setStr := strings.Join(fields, ", ")
	insertSql := fmt.Sprintf("%s INTO %s SET %s", insertType, tblName, setStr)
	result, err := databaseTx.tx.Exec(insertSql, values...)
	if err != nil {
		panic(err)
	}
	var lastInsertId int64
	lastInsertId, _ = result.LastInsertId()
	return lastInsertId, err
}

// 以事务方式，新增数据至mysql
// 要新增的数据以map形式传入
// 返回sql.Result以及error
func (databaseTx *DatabaseTx) Insert(data map[string]interface{}, tblName string) (int64, error) {
	return databaseTx.internalInsert(data, tblName, "INSERT")
}

// 以事务方式，以INSERT IGNORE INTO形式新增数据至mysql
// 要新增的数据以map形式传入
// 返回sql.Result以及error
func (databaseTx *DatabaseTx) Ignore(data map[string]interface{}, tblName string) (int64, error) {
	return databaseTx.internalInsert(data, tblName, "INSERT IGNORE")
}

// 以事务方式，以REPLACE INTO形式新增数据至mysql
// 要新增的数据以map形式传入
// 返回sql.Result以及error
func (databaseTx *DatabaseTx) Replace(data map[string]interface{}, tblName string) (int64, error) {
	return databaseTx.internalInsert(data, tblName, "REPLACE")
}

// 以事务方式更新mysql数据
// 要更新的数据以map形式传入
// UPDATE的WHERE语句以字符串形式传入，支持传入where语句参数，占位符为 ? ,会自动转义
// 返回sql.Result以及error
func (databaseTx *DatabaseTx) Update(data map[string]interface{}, tblName string, whereStr string, whereArgs ...interface{}) (int64, error) {
	fields := make([]string, 0, 10)
	values := make([]interface{}, 0, 10)
	for field, value := range data {
		var subField = fmt.Sprintf("`%s` = ?", field)
		fields = append(fields, subField)
		values = append(values, value)
	}
	setStr := strings.Join(fields, ", ")
	updateSql := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tblName, setStr, whereStr)
	if len(whereArgs) > 0 {
		values = append(values, whereArgs...)
	}
	result, err := databaseTx.tx.Exec(updateSql, values...)
	if err != nil {
		panic(err)
	}
	var affectedRows int64
	affectedRows, _ = result.RowsAffected()
	return affectedRows, err
}

// 以事务方式删除mysql数据
// DELETE FROM的WHERE语句以字符串形式传入，支持传入where语句参数，占位符为 ? ,会自动转义
// 返回sql.Result以及error
func (databaseTx *DatabaseTx) Delete(tblName string, whereStr string, whereArgs ...interface{}) (int64, error) {
	deleteSql := fmt.Sprintf("DELETE FROM %s WHERE %s", tblName, whereStr)
	result, err := databaseTx.tx.Exec(deleteSql, whereArgs...)
	if err != nil {
		panic(err)
	}
	var affectedRows int64
	affectedRows, _ = result.RowsAffected()
	return affectedRows, err
}