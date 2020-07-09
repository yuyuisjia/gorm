package gorm

import (
	"crypto/sha1"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

var dmsqlIndexRegex = regexp.MustCompile(`^(.+)\((\d+)\)$`)

type dmsql struct {
	commonDialect
}

func init() {
	RegisterDialect("dm", &dmsql{})
}

func (dmsql) GetName() string {
	return "dmsql"
}

func (dmsql) Quote(key string) string {
	return fmt.Sprintf("`%s`", key)
}

func (d *dmsql) DataTypeOf(field *StructField) string {
	var dataValue, sqlType, size, additionalType = ParseFieldStructForDialect(field, d)

	// dmsql allows only one auto increment column per table, and it must
	// be a KEY column.
	if _, ok := field.TagSettingsGet("AUTO_INCREMENT"); ok {
		if _, ok = field.TagSettingsGet("INDEX"); !ok && !field.IsPrimaryKey {
			field.TagSettingsDelete("AUTO_INCREMENT")
		}
	}

	if sqlType == "" {
		switch dataValue.Kind() {
		case reflect.Bool:
			sqlType = "bit"
		case reflect.Int8:
			if d.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "tinyint AUTO_INCREMENT"
			} else {
				sqlType = "tinyint"
			}
		case reflect.Int, reflect.Int16, reflect.Int32:
			if d.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "int AUTO_INCREMENT"
			} else {
				sqlType = "int"
			}
		case reflect.Uint8:
			if d.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "tinyint unsigned AUTO_INCREMENT"
			} else {
				sqlType = "tinyint unsigned"
			}
		case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uintptr:
			if d.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "int unsigned AUTO_INCREMENT"
			} else {
				sqlType = "int unsigned"
			}
		case reflect.Int64:
			if d.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "bigint AUTO_INCREMENT"
			} else {
				sqlType = "bigint"
			}
		case reflect.Uint64:
			if d.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "bigint unsigned AUTO_INCREMENT"
			} else {
				sqlType = "bigint unsigned"
			}
		case reflect.Float32, reflect.Float64:
			sqlType = "double"
		case reflect.String:
			if size > 0 && size < 65532 {
				sqlType = fmt.Sprintf("varchar(%d)", size)
			} else {
				sqlType = "longtext"
			}
		case reflect.Struct:
			if _, ok := dataValue.Interface().(time.Time); ok {
				precision := ""
				if p, ok := field.TagSettingsGet("PRECISION"); ok {
					precision = fmt.Sprintf("(%s)", p)
				}

				if _, ok := field.TagSettings["NOT NULL"]; ok || field.IsPrimaryKey {
					sqlType = fmt.Sprintf("DATETIME%v", precision)
				} else {
					sqlType = fmt.Sprintf("DATETIME%v NULL", precision)
				}
			}
		default:
			if IsByteArrayOrSlice(dataValue) {
				if size > 0 && size < 65532 {
					sqlType = fmt.Sprintf("varbinary(%d)", size)
				} else {
					sqlType = "longblob"
				}
			}
		}
	}

	if sqlType == "" {
		panic(fmt.Sprintf("invalid sql type %s (%s) in field %s for dmsql", dataValue.Type().Name(), dataValue.Kind().String(), field.Name))
	}

	if strings.TrimSpace(additionalType) == "" {
		return sqlType
	}
	return fmt.Sprintf("%v %v", sqlType, additionalType)
}

func (d dmsql) RemoveIndex(tableName string, indexName string) error {
	_, err := d.db.Exec(fmt.Sprintf("DROP INDEX %v ON %v", indexName, d.Quote(tableName)))
	return err
}

func (d dmsql) ModifyColumn(tableName string, columnName string, typ string) error {
	_, err := d.db.Exec(fmt.Sprintf("ALTER TABLE %v MODIFY COLUMN %v %v", tableName, columnName, typ))
	return err
}

func (d dmsql) LimitAndOffsetSQL(limit, offset interface{}) (sql string, err error) {
	if limit != nil {
		parsedLimit, err := d.parseInt(limit)
		if err != nil {
			return "", err
		}
		if parsedLimit >= 0 {
			sql += fmt.Sprintf(" LIMIT %d", parsedLimit)

			if offset != nil {
				parsedOffset, err := d.parseInt(offset)
				if err != nil {
					return "", err
				}
				if parsedOffset >= 0 {
					sql += fmt.Sprintf(" OFFSET %d", parsedOffset)
				}
			}
		}
	}
	return
}

func (d dmsql) HasForeignKey(tableName string, foreignKeyName string) bool {
	var count int
	_, tableName = currentDatabaseAndTable(&d, tableName)
	d.db.QueryRow("SELECT count(*) FROM USER_CONSTRAINTS WHERE TABLE_NAME=? AND CONSTRAINT_NAME=? AND CONSTRAINT_TYPE='R'", tableName, foreignKeyName).Scan(&count)
	return count > 0
}

func (d dmsql) HasTable(tableName string) bool {
	currentTableSpace, tableName := currentDatabaseAndTable(&d, tableName)
	var count int
	d.db.QueryRow(fmt.Sprintf("SELECT count(*) FROM USER_TABLES WHERE TABLESPACE = `%s` AND TABLE_NAME = `%s` ", currentTableSpace, tableName)).Scan(&count)
	return count > 0
}

func (d dmsql) HasIndex(tableName string, indexName string) bool {
	currentTableSpace, tableName := currentDatabaseAndTable(&d, tableName)
	var count int
	d.db.QueryRow(fmt.Sprintf("SELECT count(*) FROM USER_INDEXS WHERE TABLESPACE = `%s` AND TABLE_NAME = `%s` AND INDEX_NAME = `%s` ", currentTableSpace, tableName, indexName)).Scan(&count)
	//if rows, err := d.db.Query(fmt.Sprintf("SHOW INDEXES FROM `%s` FROM `%s` WHERE Key_name = ?", tableName, currentTableSpace), indexName); err != nil {
	//	panic(err)
	//} else {
	//	defer rows.Close()
	//	return rows.Next()
	//}
	return count > 0
}

func (d dmsql) HasColumn(tableName string, columnName string) bool {
	_, tableName = currentDatabaseAndTable(&d, tableName)
	var count int
	d.db.QueryRow(fmt.Sprintf("SELECT count(*) FROM USER_TAB_COLUMNS WHERE TABLE_NAME = `%s` AND COLUMN_NAME =`%s` ", tableName, columnName)).Scan(&count)
	return count > 0
	//if rows, err := d.db.Query(fmt.Sprintf("SHOW COLUMNS FROM `%s` FROM `%s` WHERE Field = ?", tableName, currentDatabase), columnName); err != nil {
	//	panic(err)
	//} else {
	//	defer rows.Close()
	//	return rows.Next()
	//}
}

//CurrentDatabase tablespace
func (d dmsql) CurrentDatabase() (name string) {
	//d.db.QueryRow("SELECT DATABASE()").Scan(&name)
	name = "blockchain"
	return
}

func (dmsql) SelectFromDummyTable() string {
	return "FROM DUAL"
}

func (d dmsql) BuildKeyName(kind, tableName string, fields ...string) string {
	keyName := d.commonDialect.BuildKeyName(kind, tableName, fields...)
	if utf8.RuneCountInString(keyName) <= 64 {
		return keyName
	}
	h := sha1.New()
	h.Write([]byte(keyName))
	bs := h.Sum(nil)

	// sha1 is 40 characters, keep first 24 characters of destination
	destRunes := []rune(keyNameRegex.ReplaceAllString(fields[0], "_"))
	if len(destRunes) > 24 {
		destRunes = destRunes[:24]
	}

	return fmt.Sprintf("%s%x", string(destRunes), bs)
}

// NormalizeIndexAndColumn returns index name and column name for specify an index prefix length if needed
func (dmsql) NormalizeIndexAndColumn(indexName, columnName string) (string, string) {
	submatch := dmsqlIndexRegex.FindStringSubmatch(indexName)
	if len(submatch) != 3 {
		return indexName, columnName
	}
	indexName = submatch[1]
	columnName = fmt.Sprintf("%s(%s)", columnName, submatch[2])
	return indexName, columnName
}

func (dmsql) DefaultValueStr() string {
	return "VALUES()"
}