package oracle

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"strconv"

	"github.com/leocomelli/gorm"
	_ "github.com/leocomelli/goracle"
)

type SequenceStore struct {
	TableName string  `gorm:"primary_key"`
	Sequence  float64 `gorm:"not null;`
}

var sequenceStoreExists bool

func buildFakeSequenceGenerator(sequenceName string, scope *gorm.Scope) (float64, bool) {

	if sequenceName != "AUTO_INCREMENT" {
		return 0, false
	}

	scope.Log("buildFakeSequenceGenerator: Do not use this feature in a production environment. You must create your own sequence!")

	var nextVal float64 = 0
	if !sequenceStoreExists {
		scope.NewDB().CreateTable(&SequenceStore{})
		sequenceStoreExists = true
	}

	var r SequenceStore
	scope.NewDB().Model(&SequenceStore{}).Where("TABLE_NAME = ?", scope.QuotedTableName()).Scan(&r)

	if len(r.TableName) == 0 {
		r.TableName = scope.QuotedTableName()
		r.Sequence = 0
	}
	r.Sequence += 1
	scope.NewDB().Save(&r)

	nextVal = r.Sequence

	return nextVal, true
}

func setIdentityInsert(scope *gorm.Scope) {

	if scope.Dialect().GetName() == "goracle" {
		for _, field := range scope.PrimaryFields() {
			if seq, ok := field.TagSettings["AUTO_INCREMENT"]; ok && field.IsBlank {

				var nextVal float64
				if nextVal, ok = buildFakeSequenceGenerator(seq, scope); !ok {
					stmt := fmt.Sprintf("SELECT %s.nextval %s", seq, scope.Dialect().SelectFromDummyTable())

					row := scope.NewDB().Raw(stmt).Row()
					row.Scan(&nextVal)
				}
				scope.SetColumn(field.Name, nextVal)
				scope.InstanceSet("oracle:sequence_insert_on", true)
			}
		}
	}
}

type oracle struct {
	db gorm.SQLCommon
	gorm.DefaultForeignKeyNamer
}

func init() {
	gorm.DefaultCallback.Create().After("gorm:begin_transaction").Register("oracle:set_identity_insert", setIdentityInsert)
	gorm.RegisterDialect("goracle", &oracle{})
}

func (oracle) GetName() string {
	return "goracle"
}

func (d *oracle) SetDB(db gorm.SQLCommon) {
	d.db = db
}

func (oracle) BindVar(i int) string {
	return fmt.Sprintf(":%d", i)
}

func (oracle) Quote(key string) string {
	return fmt.Sprintf("%s", strings.ToUpper(key))
}

func (d *oracle) DataTypeOf(field *gorm.StructField) string {
	var dataValue, sqlType, size, additionalType = gorm.ParseFieldStructForDialect(field, d)

	if sqlType == "" {
		switch dataValue.Kind() {
		case reflect.Bool:
			sqlType = "CHAR(1)"
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uintptr:
			sqlType = "INTEGER"
		case reflect.Int64, reflect.Uint64:
			sqlType = "NUMBER"
		case reflect.Float32, reflect.Float64:
			sqlType = "FLOAT"
		case reflect.String:
			if size > 0 && size < 4000 {
				sqlType = fmt.Sprintf("VARCHAR(%d)", size)
			} else {
				sqlType = "CLOB"
			}
		case reflect.Struct:
			if _, ok := dataValue.Interface().(time.Time); ok {
				sqlType = "DATE"
			}
		default:
			if gorm.IsByteArrayOrSlice(dataValue) {
				sqlType = "BLOB"
			}
		}
	}

	if sqlType == "" {
		panic(fmt.Sprintf("invalid sql type %s (%s) for ora", dataValue.Type().Name(), dataValue.Kind().String()))
	}

	if strings.TrimSpace(additionalType) == "" {
		return sqlType
	}
	return fmt.Sprintf("%v %v", sqlType, additionalType)
}

func (d oracle) HasIndex(tableName string, indexName string) bool {
	var count int
	d.db.QueryRow("SELECT COUNT(1) FROM USER_INDEXES WHERE TABLE_NAME = :1 AND INDEX_NAME = :2", strings.ToUpper(tableName), strings.ToUpper(indexName)).Scan(&count)
	return count > 0
}

func (d oracle) RemoveIndex(tableName string, indexName string) error {
	_, err := d.db.Exec(fmt.Sprintf("DROP INDEX %v", indexName))
	return err
}

func (d oracle) HasForeignKey(tableName string, foreignKeyName string) bool {
	var count int
	d.db.QueryRow("SELECT COUNT(1) FROM USER_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'R' AND TABLE_NAME = :1 AND CONSTRAINT_NAME = :2", strings.ToUpper(tableName), strings.ToUpper(foreignKeyName)).Scan(&count)
	return count > 0
}

func (d oracle) HasTable(tableName string) bool {
	var count int
	d.db.QueryRow("SELECT COUNT(1) FROM USER_TABLES WHERE TABLE_NAME = :1", strings.ToUpper(tableName)).Scan(&count)
	return count > 0
}

func (d oracle) HasColumn(tableName string, columnName string) bool {
	var count int
	d.db.QueryRow("SELECT COUNT(1) FROM USER_TAB_COLUMNS WHERE TABLE_NAME = :1 AND COLUMN_NAME = :2", strings.ToUpper(tableName), strings.ToUpper(columnName)).Scan(&count)
	return count > 0
}

func (d oracle) CurrentDatabase() (name string) {
	d.db.QueryRow("SELECT GLOBAL_NAME FROM GLOBAL_NAME").Scan(&name)
	return
}

func (oracle) LimitWhereSQL(limit interface{}) (sql string) {
	if limit != nil {
		if parsedLimit, err := strconv.ParseInt(fmt.Sprint(limit), 0, 0); err == nil && parsedLimit >= 0 {
			sql = fmt.Sprintf(" AND (ROWNUM <= %d)", parsedLimit)
		}
	}
	return
}

func (oracle) LimitAndOffsetSQL(limit, offset interface{}) (sql string) {
	return
}

func (oracle) SelectFromDummyTable() string {
	return "FROM dual"
}

func (oracle) LastInsertIDReturningSuffix(tableName, key string) string {
	return ""
}
