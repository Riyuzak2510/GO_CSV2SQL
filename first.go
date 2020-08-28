
package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"os"
	"runtime"
	"time"
)

func Insertdb(db *sql.DB,status *bool,cnt *int,values [][]string,t1 time.Time)	{
	vals := []interface{}{}
	sqlstr := "insert into Industries(year,industry_code_ANZSIC,industry_name_ANZSIC,rme_size_grp,variable,value,unit) values "
	cnt1 := 0
	for cnt1 < 1000 {
		if *cnt == len(values){
			*status = false;
			break;
		}
		record := values[*cnt]
		// Stop at EOF.
		sqlstr += "(?, ?, ?, ?, ?, ?, ?),"
		vals = append(vals,record[0],record[1],record[2],record[3],record[4],record[5],record[6])
		*cnt++
		cnt1++
	}
	sqlstr = sqlstr[0:len(sqlstr)-1]
	stmt, _ := db.Prepare(sqlstr)
	res, _ := stmt.Exec(vals...)
	res = res
	/*t2 := time.Now()
	fmt.Println(t2.Sub(t1))
	 */
	return
}
func ReadCsvFile(db *sql.DB,values [][]string)  {
	// Load a csv file.
	cnt := 0
	status := true
	for {
			//fmt.Println(runtime.NumGoroutine())
			if cnt == 0 {
				cnt++
				continue
			}
			for runtime.NumGoroutine() < 8	{

				// Display record.
				// ... Display record length.
				// ... Display all individual elements of the slice.
				//fmt.Println(record)
				t1 := time.Now()
				go Insertdb(db,&status,&cnt,values,t1)
			}
			if !status {
				break
			}
		}
	return
}
func say(s1 string){
	for i := 0; i < 10; i++{
	fmt.Println(s1);
	time.Sleep(time.Second*1)
	}
}
func ProcessCsv(filePath string,values* [][]string)  {
	// Load a csv file.
	f, _ := os.Open(filePath)
	// Create a new reader.
	r := csv.NewReader(f)
	for {
		record, err := r.Read()
		// Stop at EOF.
		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}
		// Display record.
		// ... Display record length.
		// ... Display all individual elements of the slice.
		//var values[][] string
		*values = append(*values,record)
		//fmt.Println(len(values))
	}
}
func main(){
	s1 := "/home/mayank/Projects/Project-1/Book1.csv"
	db, err := sql.Open("mysql", "mayank:new-strong-password@tcp(localhost:3306)/testdb")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	runtime.GOMAXPROCS(runtime.NumCPU() - 1)
	if err != nil {
		panic(err.Error())
	}
	var values [][]string
	fmt.Println(time.Now())
	ProcessCsv(s1,&values)
	fmt.Println("Connection done!")
	ReadCsvFile(db,values)
	fmt.Println(time.Now())
}


