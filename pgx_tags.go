package pgx_tags

import (
	"fmt"
	"context"
	"reflect"
	"strings"
	"github.com/jackc/pgx/v4/pgxpool"
)

var placeholder_sep = "$%d,"

func SetPlaceHolderSeperator(sep string) {
	placeholder_sep = sep
}

func GetPlaceHolderSeperator() string {
	return placeholder_sep
}

var struct_tag = "sql"

func SetStructTag(tag string) {
	struct_tag = tag
}

func GetStructTag() string {
	return struct_tag
}

var dbpool *pgxpool.Pool

func SetDBPool(pool *pgxpool.Pool) {
	dbpool = pool
}

type TagQuery struct {
	Struct interface{}
	Value reflect.Value
	Table string

	Fields *FieldDesc

	qry_custom string
	qry_select string
	qry_insert string
}

type FieldDesc struct {
	Placeholders string
	Columns	string
	Updates string
	Numbers []int
}

/*Never forget to use a pointer as prm*/
func CreateTagQuery(v interface{}, ignore map[string]int, table string) *TagQuery {
	s := reflect.ValueOf(v).Elem()
	st := s.Type()

	var fe_sb, ft_sb, fu_sb strings.Builder
	fieldcounter := int(0)
	fieldnumbers := make([]int, st.NumField()-len(ignore))
	ignored_all := false
	ignored_cnt := int(0)
	if ignored_cnt == len(ignore) {
		ignored_all = true
	}
	for i := 0; i < st.NumField(); i++ {
		tag := st.Field(i).Tag.Get(struct_tag)
		if tag == "-" {
			fieldnumbers = fieldnumbers[:len(fieldnumbers)-1]
			continue
		}
		if !ignored_all {
			_, ignored := ignore[tag]
			if ignored {
				ignored_cnt += 1
				if ignored_cnt == len(ignore) {
					ignored_all = true
				}
				continue
			}
		}

		ft_sb.WriteString(tag)
		ft_sb.WriteString(",")
		fieldnumbers[fieldcounter] = i
		fieldcounter += 1
		fmt.Fprintf(&fe_sb, placeholder_sep, fieldcounter)
		fmt.Fprintf(&fu_sb, tag+"="+placeholder_sep, fieldcounter)
	}

	fieldtags := ft_sb.String()
	fieldenum := fe_sb.String()
	fieldupdate := fu_sb.String()
	fieldenum = fieldenum[:len(fieldenum)-1]
	fieldtags = fieldtags[:len(fieldtags)-1]
	fieldupdate = fieldupdate[:len(fieldupdate)-1]
	return &TagQuery{Struct:s.Interface(), Value:s, Table:table, Fields:&FieldDesc{Placeholders:fieldenum, Columns:fieldtags, Updates:fieldupdate, Numbers:fieldnumbers}}
}

/*Never forget to use a pointer as prm*/
func CreateTagQueryOfFields(v interface{}, fields map[string]int, table string) *TagQuery {
	s := reflect.ValueOf(v).Elem()
	st := s.Type()

	var fe_sb, ft_sb, fu_sb strings.Builder
	fieldcounter := int(0)
	fieldnumbers := make([]int, len(fields))

	for i := 0; i < st.NumField(); i++ {
		tag := st.Field(i).Tag.Get(struct_tag)
		if tag == "-" {
			continue
		}
		_, finding := fields[tag]
		if finding {
			ft_sb.WriteString(tag)
			ft_sb.WriteString(",")
			fieldnumbers[fieldcounter] = i
			fieldcounter += 1
			fmt.Fprintf(&fe_sb, placeholder_sep, fieldcounter)
			fmt.Fprintf(&fu_sb, tag+"="+placeholder_sep, fieldcounter)
			if fieldcounter == len(fields) {
				break
			}
		}
	}

	fieldtags := ft_sb.String()
	fieldenum := fe_sb.String()
	fieldupdate := fu_sb.String()
	fieldenum = fieldenum[:len(fieldenum)-1]
	fieldtags = fieldtags[:len(fieldtags)-1]
	fieldupdate = fieldupdate[:len(fieldupdate)-1]
	return &TagQuery{Struct:s.Interface(), Value:s, Table:table, Fields:&FieldDesc{Placeholders:fieldenum, Columns:fieldtags, Updates:fieldupdate, Numbers:fieldnumbers}}
}

/*Never forget to use a pointer as prm*/
func (tq *TagQuery) GetCopyWithStruct(v interface{}) *TagQuery {
	new_tq := tq
	s := reflect.ValueOf(v).Elem()
	new_tq.Struct = s.Interface()
	new_tq.Value = s
	return new_tq
}

func (tq *TagQuery) GetReflectedMembersOf(v interface{}) []interface{} {
	s := reflect.ValueOf(v)
	mem_slice := make([]interface{}, len(tq.Fields.Numbers))
	for index, key := range tq.Fields.Numbers {
		mem_slice[index] = s.Field(key).Interface()
	}
	return mem_slice
}

func (tq *TagQuery) GetReflectedMembers() []interface{} {
	s := reflect.ValueOf(tq.Struct)
	mem_slice := make([]interface{}, len(tq.Fields.Numbers))
	for index, key := range tq.Fields.Numbers {
		mem_slice[index] = s.Field(key).Interface()
	}
	return mem_slice
}

/*Never forget to use this function with a pointer*/
func (tq *TagQuery) GetReflectedAddrOf(v interface{}) (reflect.Value, []interface{}) {
	s := reflect.ValueOf(v).Elem()
	addr_slice := make([]interface{}, len(tq.Fields.Numbers))
	for index, key := range tq.Fields.Numbers {
		field := s.Field(key)
		if field.CanAddr() {
			addr_slice[index] = field.Addr().Interface()
	  }
	}
	return s, addr_slice
}

func (tq *TagQuery) GetReflectedAddr() (reflect.Value, []interface{}) {
	s := tq.Value
	addr_slice := make([]interface{}, len(tq.Fields.Numbers))
	for index, key := range tq.Fields.Numbers {
		field := s.Field(key)
		if field.CanAddr() {
			addr_slice[index] = field.Addr().Interface()
	  }
	}
	return s, addr_slice
}

func (tq *TagQuery) formInsert() {
	if len(tq.qry_insert) == 0 {
		var qry_sb strings.Builder
		qry_sb.WriteString("insert into ")
		qry_sb.WriteString(tq.Table)
		qry_sb.WriteString("(")
		qry_sb.WriteString(tq.Fields.Columns)
		qry_sb.WriteString(") values (")
		qry_sb.WriteString(tq.Fields.Placeholders)
		qry_sb.WriteString(");")
		tq.qry_insert = qry_sb.String()
	}
}

func (tq *TagQuery) formInsertCustom(add string) {
	var qry_sb strings.Builder
	qry_sb.WriteString("insert into ")
	qry_sb.WriteString(tq.Table)
	qry_sb.WriteString("(")
	qry_sb.WriteString(tq.Fields.Columns)
	qry_sb.WriteString(") values (")
	qry_sb.WriteString(tq.Fields.Placeholders)
	qry_sb.WriteString(")")
	qry_sb.WriteString(" ")
	qry_sb.WriteString(add+";")
	tq.qry_custom = qry_sb.String()
}

func (tq *TagQuery) Insert() error {
	tq.formInsert()
	_, err := dbpool.Exec(context.Background(), tq.qry_insert, tq.GetReflectedMembers()...)
	return err
}

/*Normalerweise umbauen InsertGetField*/
func (tq *TagQuery) InsertGetID() (uint64, error) {
	tq.formInsertCustom("on conflict do nothing returning id")
	var id uint64
	err := dbpool.QueryRow(context.Background(), tq.qry_custom, tq.GetReflectedMembers()...).Scan(&id)
	return id, err
}

func (tq *TagQuery) formSelect() {
	if len(tq.qry_select) == 0 {
		var qry_sb strings.Builder
		qry_sb.WriteString("select ")
		qry_sb.WriteString(tq.Fields.Columns)
		qry_sb.WriteString(" from ")
		qry_sb.WriteString(tq.Table+";")
		tq.qry_select = qry_sb.String()
	}
}

func (tq *TagQuery) formSelectCustom(add string) {
	var qry_sb strings.Builder
	qry_sb.WriteString("select ")
	qry_sb.WriteString(tq.Fields.Columns)
	qry_sb.WriteString(" from ")
	qry_sb.WriteString(tq.Table)
	qry_sb.WriteString(" ")
	qry_sb.WriteString(add+";")
	tq.qry_custom = qry_sb.String()
}

func (tq *TagQuery) formSelectAll() {
	var qry_sb strings.Builder
	qry_sb.WriteString("select * from ")
	qry_sb.WriteString(tq.Table+";")
	tq.qry_custom = qry_sb.String()
}

func (tq *TagQuery) Select() ([]interface{}, error) {
	tq.formSelect()
	abp := []interface{}{}
	rows, err := dbpool.Query(context.Background(), tq.qry_select)
	if err != nil {
		return abp, err
	}
	defer rows.Close()
	for rows.Next() {
		i, addr_slice := tq.GetReflectedAddr()
		err = rows.Scan(addr_slice...)
		if err != nil {
			return abp, err
		}
		abp = append(abp, i.Interface())
	}
	err = rows.Err()
	if err != nil {
		return abp, err
	}
	return abp, nil
}

func (tq *TagQuery) SelectByID(id uint64) (interface{}, error) {
	tq.formSelectCustom("where id=$1")
	i, addr_slice := tq.GetReflectedAddr()
	err := dbpool.QueryRow(context.Background(), tq.qry_custom, id).Scan(addr_slice...)
	return i.Interface(), err
}

func (tq *TagQuery) SelectCustom(custom string, args ...interface{}) ([]interface{}, error) {
	tq.formSelectCustom(custom)
	return tq.SelectCommon(tq.qry_custom, args)
}

/*produces error, when QueryTag has ignored fields*/
func (tq *TagQuery) SelectAll() ([]interface{}, error) {
	tq.formSelectAll()
	return tq.SelectCommon(tq.qry_custom)
}

func (tq *TagQuery) SelectCommon(qry string, args ...interface{}) ([]interface{}, error) {
	abp := []interface{}{}
	rows, err := dbpool.Query(context.Background(), qry, args)
	if err != nil {
		return abp, err
	}
	defer rows.Close()
	for rows.Next() {
		i, addr_slice := tq.GetReflectedAddr()
		err = rows.Scan(addr_slice...)
		if err != nil {
			return abp, err
		}
		abp = append(abp, i.Interface())
	}
	err = rows.Err()
	if err != nil {
		return abp, err
	}
	return abp, nil
}

func (tq *TagQuery) Update(where string) error {
	var cnt_sb strings.Builder
	cnt_sb.WriteString("update ")
	cnt_sb.WriteString(tq.Table)
	cnt_sb.WriteString(" set ")
	cnt_sb.WriteString(tq.Fields.Updates)
	if len(where) > 0 {
		cnt_sb.WriteString(where)
	}
	cnt_sb.WriteString(";")
	_, err := dbpool.Exec(context.Background(), cnt_sb.String(), tq.GetReflectedMembers()...)
	return err
}

func (tq *TagQuery) UpdateFieldWith(field string, where string, args ...interface{},) error {
	var cnt_sb strings.Builder
	cnt_sb.WriteString("update ")
	cnt_sb.WriteString(tq.Table)
	cnt_sb.WriteString(" set ")
	cnt_sb.WriteString(field)
	cnt_sb.WriteString("=$1 ")
	if len(where) > 0 {
		cnt_sb.WriteString(where)
	}
	cnt_sb.WriteString(";")
	_, err := dbpool.Exec(context.Background(), cnt_sb.String(), args...)
	return err
}

func (tq *TagQuery) Count(where string) (uint64, error) {
	var cnt_sb strings.Builder
	cnt_sb.WriteString("SELECT COUNT(*) FROM ")
	cnt_sb.WriteString(tq.Table)
	if len(where) > 0 {
		cnt_sb.WriteString(where)
	}
	cnt_sb.WriteString(";")
	var count uint64
	err := dbpool.QueryRow(context.Background(), cnt_sb.String()).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}
