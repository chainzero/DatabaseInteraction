package main

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type structQueryColumnList struct {
	orgValue         string
	matchedDestTable string
}

type structMatchedColumns struct {
	orgValue         string
	matchedDestTable string
	matchesDestCol   string
}

var strSQLJoinSyntax string

func main() {

	//############Open database connection###############################################
	//Removing password from string for GitHub upload */
	db, err := sql.Open("mysql", "root:XXXX!@tcp(10.70.110.10:3306)/collab_db_schema")

	if err != nil {
		panic(err.Error())
	}

	defer db.Close()
	//###################################################################################

	//<ID:UserRequestViaSQL>###WebEx Teams user request for ConnectWise intel is written to DB in Node-Red and now ingested

	if err != nil {
		panic(err.Error())
	}

	defer db.Close()

	varSQLQueryUserUtterance := ("select user_room_id, user_current_request from connectwise_db_user_requests ORDER BY ID DESC limit 1")

	rowUserRequest, _ := db.Query(varSQLQueryUserUtterance)

	varTeamsUserID := "null"
	stringUserInput := "null"

	for rowUserRequest.Next() {
		varTeamsUserID = "user_id"
		stringUserInput = "user_current_request"

		err := rowUserRequest.Scan(&varTeamsUserID, &stringUserInput)

		if err != nil {
			panic(err.Error())
		}
	}

	//</ID:UserRequestViaSQL>###########################################################

	//<ID:UserInputTo Variable>#######Ingest user request and parse fields into correct syntax and into variable#############
	// Isolate Primary Table Name.  In current version this relies on receiving
	// a user input string that contains the syntax of <table_name_varible> with <with_search_text>.  Such as in example
	//"device with description" the primary table is device.
	//stringUserInput = "connectwise intel " + stringUserInput
	reIsolatePriTable := regexp.MustCompile("with")
	isolatePriTableTemp := reIsolatePriTable.Split(stringUserInput, 100)
	isolatePriTableTemp2 := isolatePriTableTemp[0]
	reIsolatePriTable2 := regexp.MustCompile(" ")
	isolatePriTableTemp3 := reIsolatePriTable2.Split(isolatePriTableTemp2, 100)
	isolatePriTable := isolatePriTableTemp3[0]
	varDateTimeSearchStart := "null"
	strFinalSQLWithUnions := "null"

	//SQL query to determine precise source table determination
	varSQLQueryPrimaryTables := ("select sourceTable from connectwise_db_schema_mappings where sourceTable like '%" + isolatePriTable + "'")
	rowPriTable, _ := db.Query(varSQLQueryPrimaryTables)
	defer rowPriTable.Close()

	isolatePriTable = "sourceTable"

	for rowPriTable.Next() {
		if err := rowPriTable.Scan(&isolatePriTable); err != nil {
			fmt.Println(err)
			//Else If statements isolate a table beginning with SR or SO as primary search targets.  This allows correct table isolation when there are several returns
			//such as SR_Service, Authentication_Service, SRT_Service.  Without the else if statements the For loop would continue thru all matches and the last match
			//would be set as the isolate table variable and in this case it would become SRT_Service but we want SR_Service as the table used.
		} else if strings.HasPrefix(strings.ToLower(isolatePriTable), "sr_") {
			break
		} else if strings.HasPrefix(strings.ToLower(isolatePriTable), "so_") {
			break
		}
	}

	//Isolate Table Column to Search On (I.e. directory number or description).  Such as in example
	//"device with description" the primary table is device and the where in the SQL statement would be description followed
	//by search text in next section of string isolation
	reIsolateSearchField := regexp.MustCompile("with ")
	isolateSearchFieldV1 := reIsolateSearchField.Split(stringUserInput, 100)
	isolateSearchFieldTemp := isolateSearchFieldV1[1]
	reIsolateSearchFieldV2 := regexp.MustCompile(" that")
	isolateSearchFieldTempV2 := reIsolateSearchFieldV2.Split(isolateSearchFieldTemp, 100)
	isolateSearchField := isolateSearchFieldTempV2[0]

	//SQL query to determine precise table to use within where clause
	varSQLQueryWhereTables := ("select sourceTable, destinationTable from connectwise_db_schema_mappings where sourceTable = '" + isolatePriTable + "' and sourceTableColumn like '%" + isolateSearchField + "%'")
	rowWhereTable, _ := db.Query(varSQLQueryWhereTables)
	defer rowWhereTable.Close()

	varSearchSourceTable := "sourceTable"
	varSearchFieldTable := "destinationTable"

	for rowWhereTable.Next() {
		err := rowWhereTable.Scan(&varSearchSourceTable, &varSearchFieldTable)

		if err != nil {
			panic(err.Error())
			//If primary table is used in the where clause no is necessary and thus no action with else if
		}
		if varSearchSourceTable == varSearchFieldTable {
			//If primary table does not match search field - set the variable to <destination_table>.description
			//This will need to be updated for the instances in which we would want to use the name on the destination table instead of description and would allow both
			//Update for description OR name on destination table would likely need another SQL query to determine which is appropriate or use the check later in script
			//that is already doing the description or name check
		} else {
			varSQLQueryForeignTableColSearchField := ("select sourceTableColumn from connectwise_db_schema_mappings where sourceTableColumn = 'description' AND sourceTable = '" + varSearchFieldTable + "'")
			varScanForeignColSearchField := "sourceTableColumn"
			rowsForeignColumnName, _ := db.Query(varSQLQueryForeignTableColSearchField)
			defer rowsForeignColumnName.Close()
			for rowsForeignColumnName.Next() {
				err := rowsForeignColumnName.Scan(&varScanForeignColSearchField)
				if err != nil {
					fmt.Println("no match found")
					//panic(err.Error())
				}
				//If the return SQL query reveals the destination table has a description column - we assume description should be used in the where clause.
				//ConnectWise tables typically only have a description OR a _name column but not both.
				if strings.ToLower(varScanForeignColSearchField) == "description" {
					isolateSearchField = varSearchFieldTable + ".description"
					break
					//If the return SQL query reveals the destination table does not have a description column - we assume _name should be used in the where clause.
				} else if strings.Contains(varScanForeignColSearchField, "_name") {
					isolateSearchField = varSearchFieldTable + "." + varScanForeignColSearchField
					break
				}
			}

		}
	}

	//Isolate search string  (I.e. "device with description that contains Wilson" - Wilson is the search string as the last
	//name of a user's name)
	reIsolateSearchString := regexp.MustCompile("contains ")
	isolateSearchStringV1 := reIsolateSearchString.Split(stringUserInput, 100)
	isolateSearchStringTemp := isolateSearchStringV1[1]
	reIsolateSearchStringV2 := regexp.MustCompile(" ")
	isolateSearchFieldStringV2 := reIsolateSearchStringV2.Split(isolateSearchStringTemp, 100)
	isolateSearchString := isolateSearchFieldStringV2[0]

	//Isolate the columns of interest within the query.  (I.e. "device with description that contains Wilson model,
	//protocol" the columns of interest in the query would be the device model and the device protocol.  A list is built
	//with all columns of query interest and the list may be any size length allowing one or many columns to be returned.
	reIsolateColumnNames := regexp.MustCompile(isolateSearchString + " ")
	isolateColumnNamesV1 := reIsolateColumnNames.Split(stringUserInput, 100)
	isolateColumnNamesTemp := isolateColumnNamesV1[1]
	isolateColumnNamesTemp = strings.ToLower(isolateColumnNamesTemp)
	reIsolateColumnNamesV2 := regexp.MustCompile(",")
	isolateColumnNamesV2 := reIsolateColumnNamesV2.Split(isolateColumnNamesTemp, 100)
	isolateColumnNamesList := isolateColumnNamesV2
	//Sample output on section exit and using input string of company with company_name that contains cooper time, phonenbr
	//isolatePriTable = company; isolateSearchField = company_name; isolateSearchString = cooper; isolateColumnNamesList = [time  phonenbr]
	//</ID:UserInputTo Variable>###########################################################################################

	//<ID:IsolateFoeirgn Columns>#####Iterate Thru Column Names To Construct List Of Foreign Columns.  By querying the Connectwise correlation DB for all columns
	//for the source table all foreign columns are revealed as only foreign columns from the device table exist in the
	//correlation database.
	varSQLQueryCorrelation := ("select sourceTableColumn, destinationTable from connectwise_db_schema_mappings where sourceTableColumn like '%_recid' AND sourceTable = '" + strings.TrimSpace(isolatePriTable) + "'")

	//Determine number of rows in results in correlation table for iteration thru the foreign table returned from query
	varSQLQueryCorrelationCount := ("select count(*) from connectwise_db_schema_mappings where sourceTableColumn like '%_recid' AND sourceTable = '" + strings.TrimSpace(isolatePriTable) + "'")
	rowCount, _ := db.Query(varSQLQueryCorrelationCount)
	var varCount int
	for rowCount.Next() {
		if err := rowCount.Scan(&varCount); err != nil {
			fmt.Println(err)
		}
	}
	//Sample varSQLQueryCorrelation created query - select sourceTableColumn, destinationTable from connectwise_db_schema_mappings where sourceTableColumn like '%_recid' AND sourceTable = 'company'
	//Sample varCount created query - select count(*) from connectwise_db_schema_mappings where sourceTableColumn like '%_recid' AND sourceTable = 'company'
	//</ID:IsolateFoeirgn>>###################################################################################################

	//<ID:IterateUserColumnListAgainstForeignTable>#######################################################################################

	//Creation of slice that will be used to construct a slice of structs for DB columns
	sliceColumnList := make([]structQueryColumnList, 0)
	//Creation of slice that will be used to construct a slice of structs for SQL query syntax
	sliceColumnList2 := make([]structMatchedColumns, 0)

	//Utilze the type of structQueryColumnList for a new variable declaration and that will be used for individual columns
	var structQueryColumnListV2 structQueryColumnList
	var structMatchedColumnsV2 structMatchedColumns
	var strFinalSQL string
	var varConstructedTimePeriodClause string

	intForeignTableCount := 0

	//Iterate over the isolated column list from the isolation of the user deconstructed strig/utterance
	for _, v := range isolateColumnNamesList {

		//Interator for number of columns checked in CUCM DB correlation table.  Count(*) SQL query is used to determine how many
		//columns are returned - for example - in device table and for loop interates over each row until intIterator - which is
		//incremented on each loop thru - is greater than or equal to retuned column count.  At that time we drop thru that for
		//loop - proviving column does not exist in correlation table - and then send an API SQL call to CUCM to validate it is a
		//column local to the source table (I.e. local to column in device table).
		//Example of columns to iterate thru from user input sample = [time, phonenbr]

		intIterator := 1

		if err != nil {
			panic(err.Error())
		}

		rows, _ := db.Query(varSQLQueryCorrelation)
		defer rows.Close()
		varForeignTableMatch := false

		//For loop to iterate thru the SQL returned rows from the correlation table - foreign related lcoal columns only
		//Occurs with the greater For loop allowing iteration thru all user requested columns each against foreign tables
		for rows.Next() {
			varScanSourceCol := "sourceTableColumn"
			varScanDestinationTable := "destinationTable"

			err := rows.Scan(&varScanSourceCol, &varScanDestinationTable)

			if err != nil {
				panic(err.Error())
			}

			//If statements will attempt to find a match against a foreign table and once it has run thru all foreign tables
			//that are associated with the source table a local column is assumed
			//Checking varScanDestinationTable (cuurent foreign table checked) contains the user column requested
			//Match would be such as - time_zone (foreign table) contains user requested time and thus time_zone foreign table is matched
			if strings.Contains(strings.ToLower(varScanDestinationTable), strings.TrimSpace(v)) {
				structQueryColumnListV2.orgValue = v
				structQueryColumnListV2.matchedDestTable = varScanDestinationTable
				sliceColumnList = append(sliceColumnList, structQueryColumnListV2)
				varForeignTableMatch = true

				//<ID:RemoteColumnPreciseName>####For Loop determines precise remote table column
				//The purpose of this section is a determination of the precise foreign table column name to utilize
				//In the current version we are checking first if there is a column with _Name and if not match on _Name attempt
				//Description.  If either of these columns are discovered on the foreign table that column/value will be returned.
				varSQLQueryForeignTableColName := ("select sourceTableColumn from connectwise_db_schema_mappings where sourceTableColumn LIKE '%_Name' AND sourceTable = '" + varScanDestinationTable + "'")
				varScanForeignCol := "sourceTableColumn"
				rowsForeignColumnName, _ := db.Query(varSQLQueryForeignTableColName)
				defer rowsForeignColumnName.Close()
				for rowsForeignColumnName.Next() {
					err := rowsForeignColumnName.Scan(&varScanForeignCol)

					if err != nil {
						//fmt.Println("no match found")
						//panic(err.Error())
					}
				}
				if strings.Contains(varScanForeignCol, "_Name") {
					intForeignTableCount++
					structMatchedColumnsV2.orgValue = v
					structMatchedColumnsV2.matchedDestTable = varScanDestinationTable
					structMatchedColumnsV2.matchesDestCol = varScanForeignCol
					sliceColumnList2 = append(sliceColumnList2, structMatchedColumnsV2)
					break
				}

				varSQLQueryForeignTableColName = ("select sourceTableColumn from connectwise_db_schema_mappings where sourceTableColumn LIKE 'Description' AND sourceTable = '" + varScanDestinationTable + "'")
				varScanForeignCol = "sourceTableColumn"
				rowsForeignColumnName, _ = db.Query(varSQLQueryForeignTableColName)
				defer rowsForeignColumnName.Close()
				for rowsForeignColumnName.Next() {
					err := rowsForeignColumnName.Scan(&varScanForeignCol)

					if err != nil {
						//fmt.Println("no match found")
						//panic(err.Error())
					}
				}
				if strings.Contains(varScanForeignCol, "Description") {
					intForeignTableCount++
					structMatchedColumnsV2.orgValue = v
					structMatchedColumnsV2.matchedDestTable = varScanDestinationTable
					structMatchedColumnsV2.matchesDestCol = varScanForeignCol
					sliceColumnList2 = append(sliceColumnList2, structMatchedColumnsV2)
					break
				}
				//</ID:RemoteColumnPreciseName>####For Loop determines precise remote table column

				//If the current foreign table checked does not match - check to see if additional foreign tables are available tl
				//check. If we have exhausted all possible foreign tables break out of For loop allowing flow into subsequent If condition.
			} else {
				if intIterator <= varCount {
					intIterator = intIterator + 1
					continue
				} else {
					break
				}
			}
		}
		//Following If statement is encounterd when a foreign table is not matched and thus it is deemed a local column
		if varForeignTableMatch == false {
			//<ID:LocalColumnPreciseName>####For Loop determines precise local table column
			varSQLQueryCorrelationLocalColumn := ("select sourceTableColumn from connectwise_db_schema_mappings where sourceTableColumn not like '%_recid' AND sourceTable = '" + strings.TrimSpace(isolatePriTable) + "'")
			rowsLocalCol, _ := db.Query(varSQLQueryCorrelationLocalColumn)
			defer rowsLocalCol.Close()

			varScanSourceColLocalMatch := "sourceTableColumn"

			for rowsLocalCol.Next() {

				err := rowsLocalCol.Scan(&varScanSourceColLocalMatch)

				if err != nil {
					panic(err.Error())
				}

				if strings.Contains(strings.ToLower(varScanSourceColLocalMatch), strings.TrimSpace(v)) {
					structMatchedColumnsV2.orgValue = strings.TrimSpace(v)
					structMatchedColumnsV2.matchedDestTable = strings.TrimSpace(isolatePriTable)
					structMatchedColumnsV2.matchesDestCol = strings.TrimSpace(varScanSourceColLocalMatch)
					sliceColumnList2 = append(sliceColumnList2, structMatchedColumnsV2)
					break
				} else {
					continue
				}
			}
			//</ID:LocalColumnPreciseName>###example output using user test string provide would be - phonenbr

			structQueryColumnListV2.orgValue = varScanSourceColLocalMatch
			structQueryColumnListV2.matchedDestTable = isolatePriTable
			sliceColumnList = append(sliceColumnList, structQueryColumnListV2)
			//strFinalSQL = strFinalSQL + (strings.TrimSpace(structQueryColumnListV2.matchedDestTable)  + "." + strings.TrimSpace(structQueryColumnListV2.matchedDestTable))

		} //Close the For loop that iterrates thru foreign columns of source table
	} //Closes the For loop that iterates thru user requested columns
	//Example output of this section for example user string would be - in a slice of a struct - [{time Time_Zone} { phonenbr company }]
	//</ID:IterateUserColumnListAgainstForeignTable>#######################################################################################

	//<ID:SQLInsertOfColumnNamesForReportBuild>###################################################
	for intSelect, vSelect := range sliceColumnList2 {
		intSelectPlusOne := intSelect + 1
		update, err := db.Query("update connectwise_db_user_utterance_builder A INNER JOIN (select id from connectwise_db_user_utterance_builder  ORDER BY id DESC limit 1) B on A.id = B.id  SET selectColumn" + strconv.Itoa(intSelectPlusOne) + "= \"" + vSelect.matchesDestCol + "\"")
		defer update.Close()

		if err != nil {
			log.Fatal(err)
		}
	}
	//</ID:SQLInsertOfColumnNamesForReportBuild>###################################################

	//<ID:BuildFinalSQLQuerySyntaxForUserRequest>###################################################
	intLenSliceColumnList2 := len(sliceColumnList2)
	//sliceSQLJoinSyntax := make([]string, 0)
	for _, v3 := range sliceColumnList2 {
		//Local table match
		if strings.TrimSpace(v3.matchedDestTable) == strings.TrimSpace(isolatePriTable) && intLenSliceColumnList2 > 1 {
			strFinalSQL = strFinalSQL + strings.TrimSpace(v3.matchedDestTable) + "." + v3.matchesDestCol + " as " + strings.TrimSpace(v3.orgValue) + ", "
			intLenSliceColumnList2--
			//SQL insert of local table column DB iterration counter to determine most popular columns to display to user.
			insert, err := db.Query("UPDATE connectwise_db_schema_mappings SET intCountAccessed = intCountAccessed + 1 where sourceTableColumn = ? and sourceTable = ?", (strings.TrimSpace(v3.orgValue)), isolatePriTable)
			defer insert.Close()
			if err != nil {
				log.Fatal(err)
			}
			continue
		} else if strings.TrimSpace(v3.matchedDestTable) == strings.TrimSpace(isolatePriTable) {
			strFinalSQL = strFinalSQL + strings.TrimSpace(v3.matchedDestTable) + "." + v3.matchesDestCol + " as " + strings.TrimSpace(v3.orgValue)
			intLenSliceColumnList2--
			continue
			//Foreign table match
		} else if strings.TrimSpace(v3.matchedDestTable) != strings.TrimSpace(isolatePriTable) && intLenSliceColumnList2 > 1 {
			strFinalSQL = strFinalSQL + strings.TrimSpace(v3.matchedDestTable) + "." + v3.matchesDestCol + " as " + strings.TrimSpace(v3.orgValue) + ", "
			intLenSliceColumnList2--
			//SQL insert of foreign table column  - called from local table - DB iterration counter to determine most popular columns to display to user.
			strSQLJoinSyntax = strSQLJoinSyntax + (" INNER JOIN " + strings.TrimSpace(v3.matchedDestTable) + " on " + strings.TrimSpace(v3.matchedDestTable) + "." + strings.TrimSpace(v3.matchedDestTable) + "_RecID" + " = " + strings.TrimSpace(isolatePriTable) + "." + strings.TrimSpace(v3.matchedDestTable) + "_RecID")
			insert, err := db.Query("UPDATE connectwise_db_schema_mappings SET intCountAccessed = intCountAccessed + 1 where sourceTableColumn = ? and sourceTable = ?", (strings.TrimSpace(v3.matchedDestTable) + "_RecID"), isolatePriTable)
			defer insert.Close()
			if err != nil {
				log.Fatal(err)
			}
			continue
		} else if strings.TrimSpace(v3.matchedDestTable) != strings.TrimSpace(isolatePriTable) {
			strFinalSQL = strFinalSQL + strings.TrimSpace(v3.matchedDestTable) + "." + v3.matchesDestCol + " as " + strings.TrimSpace(v3.orgValue)
			intLenSliceColumnList2--
			//SQL insert of foreign table column  - called from local table - DB iterration counter to determine most popular columns to display to user.
			strSQLJoinSyntax = strSQLJoinSyntax + (" INNER JOIN " + strings.TrimSpace(v3.matchedDestTable) + " on " + strings.TrimSpace(v3.matchedDestTable) + "." + strings.TrimSpace(v3.matchedDestTable) + "_RecID" + " = " + strings.TrimSpace(isolatePriTable) + "." + strings.TrimSpace(v3.matchedDestTable) + "_RecID")
			insert, err := db.Query("UPDATE connectwise_db_schema_mappings SET intCountAccessed = intCountAccessed + 1 where sourceTableColumn = ? and sourceTable = ?", (strings.TrimSpace(v3.matchedDestTable) + "_RecID"), isolatePriTable)
			defer insert.Close()
			if err != nil {
				log.Fatal(err)
			}
		}
	} //Terminates For loop used to iterate thru column list slice

	//<ID:IncludeTimePeriodFromUser>####Add to the where of the SQL query a datetime range as captured from the user###############
	varSQLQueryTimePeriod := ("select dateTimeSearchStart, dateTimeSearchStop, dateTimeSearchField from connectwise_db_user_utterance_builder ORDER BY id DESC limit 1")
	varDateTimeSearchField := "dateTimeSearchField"
	varDateTimeSearchStart = "dateTimeSearchStart"
	varDateTimeSearchStop := "dateTimeSearchStop"
	rowsDateTimePeriod, _ := db.Query(varSQLQueryTimePeriod)
	defer rowsDateTimePeriod.Close()
	for rowsDateTimePeriod.Next() {
		err := rowsDateTimePeriod.Scan(&varDateTimeSearchStart, &varDateTimeSearchStop, &varDateTimeSearchField)
		if varDateTimeSearchStart == "dateTimeSearchStart" {
			break
		}
		varConstructedTimePeriodClause = "and " + varDateTimeSearchField + " BETWEEN '" + varDateTimeSearchStart + "' AND '" + varDateTimeSearchStop + "'"

		if err != nil {
			//fmt.Println("no match found")
			//panic(err.Error())
		}
	}
	//</ID:IncludeTimePeriodFromUser>####Add to the where of the SQL query a datetime range as captured from the user###############

	//</ID:BuildFinalSQLQuerySyntaxForUserRequest>###################################################

	if varDateTimeSearchStart != "dateTimeSearchStart" {
		strFinalSQLWithUnions = "select " + strFinalSQL + " from " + isolatePriTable + " " + strSQLJoinSyntax + " where " + isolateSearchField + " like '%" + isolateSearchString + "%' " + varConstructedTimePeriodClause + " ORDER BY " + varDateTimeSearchField + " DESC"
	} else {
		strFinalSQLWithUnions = "select " + strFinalSQL + " from " + isolatePriTable + " " + strSQLJoinSyntax + " where " + isolateSearchField + " like '%" + isolateSearchString + "%' "
	}

	fmt.Println("Final sql - " + strFinalSQL)
	fmt.Println("Primary table - " + isolatePriTable)
	fmt.Println("Join syntax - " + strSQLJoinSyntax)
	fmt.Println("Search field - " + isolateSearchField)
	fmt.Println("Search string - " + isolateSearchString)

	fmt.Println(strFinalSQLWithUnions)

}
