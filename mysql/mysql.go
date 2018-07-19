package mysql

import (
	"database/sql"
	"github.com/go-sql-driver/mysql"
	"fmt"
	"strings"
)

// 根据传入参数创建数据库链接
// 返回sql.DB以及error
func Connect(host string, username string, password string, dbname string, charset string, customParams map[string]string) (*sql.DB, error) {
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

	db, err := sql.Open("mysql", config.FormatDSN())
	return db, err
}

func internalInsert(db *sql.DB, data map[string]interface{}, tblName string, insertType string) (sql.Result, error) {
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
	result, err := db.Exec(insertSql, values...)
	return result, err
}

// 新增数据至mysql
// 要新增的数据以map形式传入
// 返回sql.Result以及error
func Insert(db *sql.DB, data map[string]interface{}, tblName string) (sql.Result, error) {
	return internalInsert(db, data, tblName, "INSERT")
}

// 以INSERT IGNORE INTO形式新增数据至mysql
// 要新增的数据以map形式传入
// 返回sql.Result以及error
func Ignore(db *sql.DB, data map[string]interface{}, tblName string) (sql.Result, error) {
	return internalInsert(db, data, tblName, "INSERT IGNORE")
}

// 以REPLACE INTO形式新增数据至mysql
// 要新增的数据以map形式传入
// 返回sql.Result以及error
func Replace(db *sql.DB, data map[string]interface{}, tblName string) (sql.Result, error) {
	return internalInsert(db, data, tblName, "REPLACE")
}

// 更新mysql数据
// 要更新的数据以map形式传入
// UPDATE的WHERE语句以字符串形式传入，支持传入where语句参数，占位符为 ? ,会自动转义
// 返回sql.Result以及error
func Update(db *sql.DB, data map[string]interface{}, tblName string, whereStr string, whereArgs ...interface{}) (sql.Result, error) {
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
	result, err := db.Exec(updateSql, values...)
	return result, err
}

// 删除mysql数据
// DELETE FROM的WHERE语句以字符串形式传入，支持传入where语句参数，占位符为 ? ,会自动转义
// 返回sql.Result以及error
func Delete(db *sql.DB, tblName string, whereStr string, whereArgs ...interface{}) (sql.Result, error) {
	deleteSql := fmt.Sprintf("DELETE FROM %s WHERE %s", tblName, whereStr)
	result, err := db.Exec(deleteSql, whereArgs...)
	return result, err
}

func txInternalInsert(tx *sql.Tx, data map[string]interface{}, tblName string, insertType string) (sql.Result, error) {
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
	result, err := tx.Exec(insertSql, values...)
	return result, err
}

// 以事务方式，新增数据至mysql
// 要新增的数据以map形式传入
// 返回sql.Result以及error
func TxInsert(tx *sql.Tx, data map[string]interface{}, tblName string) (sql.Result, error) {
	return txInternalInsert(tx, data, tblName, "INSERT")
}

// 以事务方式，以INSERT IGNORE INTO形式新增数据至mysql
// 要新增的数据以map形式传入
// 返回sql.Result以及error
func TxIgnore(tx *sql.Tx, data map[string]interface{}, tblName string) (sql.Result, error) {
	return txInternalInsert(tx, data, tblName, "INSERT IGNORE")
}

// 以事务方式，以REPLACE INTO形式新增数据至mysql
// 要新增的数据以map形式传入
// 返回sql.Result以及error
func TxReplace(tx *sql.Tx, data map[string]interface{}, tblName string) (sql.Result, error) {
	return txInternalInsert(tx, data, tblName, "REPLACE")
}

// 以事务方式更新mysql数据
// 要更新的数据以map形式传入
// UPDATE的WHERE语句以字符串形式传入，支持传入where语句参数，占位符为 ? ,会自动转义
// 返回sql.Result以及error
func TxUpdate(tx *sql.Tx, data map[string]interface{}, tblName string, whereStr string, whereArgs ...interface{}) (sql.Result, error) {
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
	result, err := tx.Exec(updateSql, values...)
	return result, err
}

// 以事务方式删除mysql数据
// DELETE FROM的WHERE语句以字符串形式传入，支持传入where语句参数，占位符为 ? ,会自动转义
// 返回sql.Result以及error
func TxDelete(tx *sql.Tx, tblName string, whereStr string, whereArgs ...interface{}) (sql.Result, error) {
	deleteSql := fmt.Sprintf("DELETE FROM %s WHERE %s", tblName, whereStr)
	result, err := tx.Exec(deleteSql, whereArgs...)
	return result, err
}