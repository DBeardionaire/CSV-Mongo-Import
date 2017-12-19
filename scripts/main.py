# import xlrd
# book = xlrd.open_workbook("C:\FileLoader\RE\RE Licensee 2501 Addr File5 0116 2.xls")
# print("The number of worksheets is {0}".format(book.nsheets))
# print("Worksheet name(s): {0}".format(book.sheet_names()))
# sh = book.sheet_by_index(0)
# print("{0} {1} {2}".format(sh.name, sh.nrows, sh.ncols))
# print("Cell D30 is {0}".format(sh.cell_value(rowx=29, colx=3)))
# for rx in range(sh.nrows):
#     print(sh.row(rx))


#!/usr/bin/python2.7
import xlrd
import xlwt
import json
import csv
import os.path

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def getColNames(sheet):
	rowSize = sheet.row_len(0)
	colValues = sheet.row_values(0, 0, rowSize )
	columnNames = []

	for value in colValues:
		columnNames.append(value)

	return columnNames

def getRowData(row, columnNames):
	rowData = {}
	counter = 0

	for cell in row:
		rowData[columnNames[counter]] = cell.value
		# rowData[columnNames[counter]] = str(cell.value)
		counter +=1

	return rowData

def getSheetData(sheet, columnNames):
	nRows = sheet.nrows
	sheetData = []
	counter = 1

	for idx in range(1, nRows):
		row = sheet.row(idx)
		rowData = getRowData(row, columnNames)
		sheetData.append(rowData)

	return sheetData

def getWorkBookData(workbook):
	nsheets = workbook.nsheets
	counter = 0
	workbookdata = {}

	for idx in range(0, nsheets):
		worksheet = workbook.sheet_by_index(idx)
		columnNames = getColNames(worksheet)
		sheetdata = getSheetData(worksheet, columnNames)
		workbookdata[worksheet.name] = sheetdata

	return workbookdata

def main():
	filename = raw_input("Enter the path to the filename -> ")

	if os.path.isfile(filename):
		workbook = xlrd.open_workbook(filename)
		workbookdata = getWorkBookData(workbook)
		output = \
		open((filename.replace("xlsx", "json")).replace("xls", "json"), "wb")
		output.write(json.dumps(workbookdata, sort_keys=True, indent=4,  separators=(',', ": ")))
		output.close()
		print "%s was created" %output.name
	else:
		print "Sorry, that was not a valid filename"


def xls_to_csv():
	filename = "C:\FileLoader\RE1217\1117DRELicensee2501EmailPhoneT-Z.xlsx"
	# filename = raw_input("Enter the path to the .xls filename -> ")

	if os.path.isfile(filename):
		x =  xlrd.open_workbook(filename)
		# x1 = x.sheet_by_name('Sheet1')
		csvfile = open((filename.replace("xlsx", "csv")).replace("xls", "csv"), 'wb')
		writecsv = csv.writer(csvfile, quoting=csv.QUOTE_ALL)

		for sheetnum in xrange(x.nsheets):
			print "%s sheetnum" %sheetnum 
			sh = x.sheet_by_index(sheetnum)
			ty = sh.cell(2,18).ctype
			print "cell type: %s" % ty 
			for rownum in xrange(sh.nrows):
				writecsv.writerow(sh.row_values(rownum))

		csvfile.close()
	else:
		print "Sorry, that was not a valid filename"

main()
# xls_to_csv()
