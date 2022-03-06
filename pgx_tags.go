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

type GetFieldDesc struct {
	Numbers []int
	Columns	string
}

type SetFieldDesc struct {
	Numbers []int
	Columns	string
	Placeholders string
	Updates string
}

type TagQuery struct {
	Struct interface{}
	Value reflect.Value
	Table string

	SetFields *SetFieldDesc
	GetFields *GetFieldDesc

	qry_custom string
	qry_select string
	qry_insert string
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
		if tag == "-" || len(tag) == 0 {
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

	setfielddesc := &SetFieldDesc{Placeholders:fieldenum, Columns:fieldtags, Updates:fieldupdate, Numbers:fieldnumbers}
	getfielddesc := &GetFieldDesc{Columns:fieldtags, Numbers:fieldnumbers}

	return &TagQuery{Struct:s.Interface(), Value:s, Table:table, SetFields:setfielddesc, GetFields:getfielddesc}
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
		if tag == "-" || len(tag) == 0 {
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

	fielddesc := &SetFieldDesc{Placeholders:fieldenum, Columns:fieldtags, Updates:fieldupdate, Numbers:fieldnumbers}
	getfielddesc := &GetFieldDesc{Columns:fieldtags, Numbers:fieldnumbers}

	return &TagQuery{Struct:s.Interface(), Value:s, Table:table, SetFields:fielddesc, GetFields:getfielddesc}
}

/*Never forget to use a pointer as prm*/
func CreateTagQueryOfSetGetFields(v interface{}, setfields map[string]int, getfields map[string]int ,table string) *TagQuery {
	s := reflect.ValueOf(v).Elem()
	st := s.Type()

	var fe_sb, ft_sb, fu_sb, gft_sb strings.Builder
	fieldcounter := int(0)
	fieldnumbers := make([]int, len(setfields))
	gfieldcounter := int(0)
	gfieldnumbers := make([]int, len(getfields))

	for i := 0; i < st.NumField(); i++ {
		tag := st.Field(i).Tag.Get(struct_tag)
		if tag == "-" || len(tag) == 0 {
			continue
		}
		_, finding := setfields[tag]
		if finding {
			ft_sb.WriteString(tag)
			ft_sb.WriteString(",")
			fieldnumbers[fieldcounter] = i
			fieldcounter += 1
			fmt.Fprintf(&fe_sb, placeholder_sep, fieldcounter)
			fmt.Fprintf(&fu_sb, tag+"="+placeholder_sep, fieldcounter)
		}
		_, getfinding := getfields[tag]
		if getfinding {
			gft_sb.WriteString(tag)
			gft_sb.WriteString(",")
			gfieldnumbers[gfieldcounter] = i
			gfieldcounter += 1
		}
	}

	fieldtags := ft_sb.String()
	gfieldtags := gft_sb.String()
	fieldenum := fe_sb.String()
	fieldupdate := fu_sb.String()
	fieldenum = fieldenum[:len(fieldenum)-1]
	fieldtags = fieldtags[:len(fieldtags)-1]
	gfieldtags = gfieldtags[:len(gfieldtags)-1]
	fieldupdate = fieldupdate[:len(fieldupdate)-1]

	fielddesc := &SetFieldDesc{Placeholders:fieldenum, Columns:fieldtags, Updates:fieldupdate, Numbers:fieldnumbers}
	getfielddesc := &GetFieldDesc{Columns:gfieldtags, Numbers:gfieldnumbers}

	return &TagQuery{Struct:s.Interface(), Value:s, Table:table, SetFields:fielddesc, GetFields:getfielddesc}
}

/*Never forget to use a pointer as prm*/
func (tq *TagQuery) GetCopyWithStruct(v interface{}) *TagQuery {
	new_tq := tq
	s := reflect.ValueOf(v).Elem()
	new_tq.Struct = s.Interface()
	new_tq.Value = s
	return new_tq
}

func (tq *TagQuery) RebuildGetFields() {return}

func (tq *TagQuery) RebuildSetFields() {return}

func (tq *TagQuery) RebuildSetGetFields(v interface{}, setfields map[string]int, getfields map[string]int) {
	s := reflect.ValueOf(v).Elem()
	tq.Struct = s.Interface()
	tq.Value = s
	st := s.Type()
	var fe_sb, ft_sb, fu_sb, gft_sb strings.Builder
	fieldcounter := int(0)
	fieldnumbers := make([]int, len(setfields))
	gfieldcounter := int(0)
	gfieldnumbers := make([]int, len(getfields))

	for i := 0; i < st.NumField(); i++ {
		tag := st.Field(i).Tag.Get(struct_tag)
		if tag == "-" || len(tag) == 0 {
			continue
		}
		_, finding := setfields[tag]
		if finding {
			ft_sb.WriteString(tag)
			ft_sb.WriteString(",")
			fieldnumbers[fieldcounter] = i
			fieldcounter += 1
			fmt.Fprintf(&fe_sb, placeholder_sep, fieldcounter)
			fmt.Fprintf(&fu_sb, tag+"="+placeholder_sep, fieldcounter)
		}
		_, getfinding := getfields[tag]
		if getfinding {
			gft_sb.WriteString(tag)
			gft_sb.WriteString(",")
			gfieldnumbers[gfieldcounter] = i
			gfieldcounter += 1
		}
	}

	fieldtags := ft_sb.String()
	gfieldtags := gft_sb.String()
	fieldenum := fe_sb.String()
	fieldupdate := fu_sb.String()
	fieldenum = fieldenum[:len(fieldenum)-1]
	fieldtags = fieldtags[:len(fieldtags)-1]
	gfieldtags = gfieldtags[:len(gfieldtags)-1]
	fieldupdate = fieldupdate[:len(fieldupdate)-1]

	tq.SetFields = &SetFieldDesc{Placeholders:fieldenum, Columns:fieldtags, Updates:fieldupdate, Numbers:fieldnumbers}
	tq.GetFields = &GetFieldDesc{Columns:gfieldtags, Numbers:gfieldnumbers}
}

func (tq *TagQuery) GetReflectedMembersOf(v interface{}) []interface{} {
	s := reflect.ValueOf(v)
	mem_slice := make([]interface{}, len(tq.SetFields.Numbers))
	for index, key := range tq.SetFields.Numbers {
		mem_slice[index] = s.Field(key).Interface()
	}
	return mem_slice
}

func (tq *TagQuery) PrintQueries() {
  fmt.Println(tq.qry_custom)
  fmt.Println(tq.qry_select)
  fmt.Println(tq.qry_insert)
}

func (tq *TagQuery) GetReflectedMembers() []interface{} {
	s := reflect.ValueOf(tq.Struct)
	mem_slice := make([]interface{}, len(tq.SetFields.Numbers))
	for index, key := range tq.SetFields.Numbers {
		mem_slice[index] = s.Field(key).Interface()
	}
	return mem_slice
}

/*Never forget to use this function with a pointer*/
func (tq *TagQuery) GetReflectedAddrOf(v interface{}) (reflect.Value, []interface{}) {
	s := reflect.ValueOf(v).Elem()
	addr_slice := make([]interface{}, len(tq.GetFields.Numbers))
	for index, key := range tq.GetFields.Numbers {
		field := s.Field(key)
		if field.CanAddr() {
			addr_slice[index] = field.Addr().Interface()
	  }
	}
	return s, addr_slice
}

func (tq *TagQuery) GetReflectedAddr() (reflect.Value, []interface{}) {
	s := tq.Value
	addr_slice := make([]interface{}, len(tq.GetFields.Numbers))
	for index, key := range tq.GetFields.Numbers {
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
		qry_sb.WriteString(tq.SetFields.Columns)
		qry_sb.WriteString(") values (")
		qry_sb.WriteString(tq.SetFields.Placeholders)
		qry_sb.WriteString(");")
		tq.qry_insert = qry_sb.String()
	}
}

func (tq *TagQuery) formInsertCustom(add string) {
	var qry_sb strings.Builder
	qry_sb.WriteString("insert into ")
	qry_sb.WriteString(tq.Table)
	qry_sb.WriteString("(")
	qry_sb.WriteString(tq.SetFields.Columns)
	qry_sb.WriteString(") values (")
	qry_sb.WriteString(tq.SetFields.Placeholders)
	qry_sb.WriteString(") ")
	qry_sb.WriteString(add+";")
	tq.qry_custom = qry_sb.String()
}

func (tq *TagQuery) FormInsertReturn() {
	if len(tq.qry_insert) == 0 {
		var qry_sb strings.Builder
		qry_sb.WriteString("insert into ")
		qry_sb.WriteString(tq.Table)
		qry_sb.WriteString("(")
		qry_sb.WriteString(tq.SetFields.Columns)
		qry_sb.WriteString(") values (")
		qry_sb.WriteString(tq.SetFields.Placeholders)
		qry_sb.WriteString(") on conflict do nothing returning ")
		qry_sb.WriteString(tq.GetFields.Columns)
		qry_sb.WriteString(";")
		tq.qry_insert = qry_sb.String()
	}
}

func (tq *TagQuery) Insert() error {
	tq.formInsert()
	_, err := dbpool.Exec(context.Background(), tq.qry_insert, tq.GetReflectedMembers()...)
	return err
}

func (tq *TagQuery) DeleteID(id uint64) error {
	_, err := dbpool.Exec(context.Background(), fmt.Sprintf("delete from %s where id = $1;", tq.Table), id)
	return err
}

func (tq *TagQuery) DeleteWhere(where string) error {
	return DeleteWhere(tq.Table, where)
}

func DeleteWhere(table, where string) error {
	_, err := dbpool.Exec(context.Background(), fmt.Sprintf("delete from %s %s;", table, where))
	return err
}

/*Normalerweise umbauen InsertGetField*/
func (tq *TagQuery) InsertGetID() (uint64, error) {
	tq.formInsertCustom("on conflict do nothing returning id")
	var id uint64
	err := dbpool.QueryRow(context.Background(), tq.qry_custom, tq.GetReflectedMembers()...).Scan(&id)
	return id, err
}

func (tq *TagQuery) InsertGetFields() (interface{}, error) {
	tq.FormInsertReturn()
	i, addr_slice := tq.GetReflectedAddr()
	err := dbpool.QueryRow(context.Background(), tq.qry_insert, tq.GetReflectedMembers()...).Scan(addr_slice...)
	return i.Interface(), err
}

func (tq *TagQuery) formSelect() {
	if len(tq.qry_select) == 0 {
		var qry_sb strings.Builder
		qry_sb.WriteString("select ")
		qry_sb.WriteString(tq.GetFields.Columns)
		qry_sb.WriteString(" from ")
		qry_sb.WriteString(tq.Table+";")
		tq.qry_select = qry_sb.String()
	}
}

func (tq *TagQuery) formSelectCustom(add string) {
	var qry_sb strings.Builder
  qry_sb.WriteString("select ")
  //fmt.Printf("%#v\n", tq.GetFields)
  //fmt.Printf("%#v\n", tq.GetFields.Columns)
	qry_sb.WriteString(tq.GetFields.Columns)
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
	return tq.SelectCommon(tq.qry_custom, args...)
}

/*produces error, when QueryTag has ignored fields*/
func (tq *TagQuery) SelectAll() ([]interface{}, error) {
	tq.formSelectAll()
	return tq.SelectCommon(tq.qry_custom)
}

//SELECT select_list FROM table_expression [ ORDER BY ... ] [ LIMIT { number | ALL } ] [ OFFSET number ] order nicht vergessen sonst willkurlich
//SELECT ... FROM fdt WHERE c1 IN (1, 2, 3)
/*
SELECT ... FROM fdt WHERE c1 IN (SELECT c1 FROM t2)
SELECT ... FROM fdt WHERE c1 IN (SELECT c3 FROM t2 WHERE c2 =
fdt.c1 + 10)
SELECT ... FROM fdt WHERE c1 BETWEEN (SELECT c3 FROM t2 WHERE c2 =
fdt.c1 + 10) AND 100
*/

/*Never forget to use this function with a pointer*/
func GetReflectedAddrOf(v interface{}) (reflect.Value, []interface{}) {
	s := reflect.ValueOf(v).Elem()
	st := s.Type()
  addr_slice := make([]interface{}, 0)

	for i := 0; i < st.NumField(); i++ {
		tag := st.Field(i).Tag.Get(struct_tag)
		if tag == "-" || len(tag) == 0 {
			continue
		}
		field := s.Field(i)
		if field.CanAddr() {
			addr_slice = append(addr_slice, field.Addr().Interface())
	  }
  }
	return s, addr_slice
}

/*Never forget to use this function with a pointer*/
func SelectWithSecondary(v interface{}, qry string, args ...interface{}) ([]interface{}, error) {
	abp := []interface{}{}
	rows, err := dbpool.Query(context.Background(), qry, args...)
	if err != nil {
    //panic(err)
		return abp, err
	}
	defer rows.Close()
	for rows.Next() {
		i, addr_slice := GetReflectedAddrOf(v)
		err = rows.Scan(addr_slice...)
		if err != nil {
      //panic(err)
			return abp, err
		}
		abp = append(abp, i.Interface())
	}
	err = rows.Err()
	if err != nil {
    //panic(err)
		return abp, err
	}
	return abp, nil
}

func (tq *TagQuery) SelectCommon(qry string, args ...interface{}) ([]interface{}, error) {
	abp := []interface{}{}
	rows, err := dbpool.Query(context.Background(), qry, args...)
	if err != nil {
    panic(err)
		return abp, err
	}
	defer rows.Close()
	for rows.Next() {
		i, addr_slice := tq.GetReflectedAddr()
		err = rows.Scan(addr_slice...)
		if err != nil {
      panic(err)
			return abp, err
		}
		abp = append(abp, i.Interface())
	}
	err = rows.Err()
	if err != nil {
    panic(err)
		return abp, err
	}
	return abp, nil
}

func (tq *TagQuery) Update(where string) error {
	var cnt_sb strings.Builder
	cnt_sb.WriteString("update ")
	cnt_sb.WriteString(tq.Table)
	cnt_sb.WriteString(" set ")
	cnt_sb.WriteString(tq.SetFields.Updates)
	if len(where) > 0 {
		cnt_sb.WriteString(where)
	}
	cnt_sb.WriteString(";")
	_, err := dbpool.Exec(context.Background(), cnt_sb.String(), tq.GetReflectedMembers()...)
	return err
}

func (tq *TagQuery) UpdateFieldWith(field string, where string, args ...interface{}) error {
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

func (tq *TagQuery) Count(where string, args ...interface{}) (uint64, error) {
	var cnt_sb strings.Builder
	cnt_sb.WriteString("select count(*) from ")
	cnt_sb.WriteString(tq.Table)
	if len(where) > 0 {
    cnt_sb.WriteString(" ")
		cnt_sb.WriteString(where)
	}
	cnt_sb.WriteString(";")
	var count uint64
	err := dbpool.QueryRow(context.Background(), cnt_sb.String(),  args...).Scan(&count)
	if err != nil {
    panic(err)
		return 0, err
	}
	return count, nil
}

func (tq *TagQuery) FieldExists(field string, arg interface{}) (bool, error) {
	var cnt_sb strings.Builder
	cnt_sb.WriteString("select exists(select 1 from ")
	cnt_sb.WriteString(tq.Table)
	cnt_sb.WriteString(" where ")
	cnt_sb.WriteString(field)
	cnt_sb.WriteString("=$1) as \"exists\";")
	var exist bool
	err := dbpool.QueryRow(context.Background(), cnt_sb.String(), arg).Scan(&exist)
	if err != nil {
		return false, err
	}
	return exist, nil
}
