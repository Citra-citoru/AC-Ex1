import pandas as pd
import pyodbc 
import csv
from pathlib import Path


def writeToCSV(name,header,data):
    with open('test-'+name+'.csv', 'w', encoding='UTF8', newline='\n') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
        f.close()

def getQuery():
    server = 'testdb.ckv1htjmgn5s.us-east-1.rds.amazonaws.com,1433' 
    database = 'TestDB' 
    username = 'test' 
    password = 'SoftDevTest123!' 
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    
    try:
        cursor = conn.cursor()
        sql1 = "SELECT [partnerName] = a.name, [Avg Loan Size] = ISNULL(AVG(l.TotalDisbursementAmount),0), [Total of Loans] = ISNULL(SUM(l.loanNumber),0), [Total Amount of Disbursed] = ISNULL(SUM(l.TotalDisbursementAmount),0), [Total of Unique Borrower] = ISNULL(b.[count],0) FROM [dbo].partners a LEFT JOIN [dbo].products p ON p.PartnerId = a.id LEFT JOIN [dbo].loans l ON l.ProductId = p.id LEFT JOIN ( SELECT TOP 100 a.id,[count]=COUNT(b.id) FROM partners a INNER JOIN Borrowers b on a.id = b.partnerId group by a.id ) b ON b.id = a.id GROUP BY a.name,b.[count] ORDER BY a.name ASC"
        sql2 = "SELECT TOP 100 b.PartnerBorrowerId, [Partner Code] = a.code, [Total Number of Loans] = ISNULL(SUM(l.loanNumber),0), [Total Amount of Disbursed] = ISNULL(SUM(l.TotalDisbursementAmount),0), [Avg Loan Size] = ISNULL(AVG(l.TotalDisbursementAmount),0) FROM partners a INNER JOIN Borrowers b ON a.id = b.PartnerId LEFT JOIN loans l on l.PartnerId = a.id GROUP BY b.PartnerBorrowerId,a.code"
        
        cursor.execute(sql1)
        data1 = cursor.fetchall()
        headers1 = [i[0] for i in cursor.description]
        
        cursor.execute(sql2)
        data2 = cursor.fetchall()
        headers2 = [i[0] for i in cursor.description]

        writeToCSV('a',headers1,data1)
        writeToCSV('b',headers2,data2)
    finally:
        cursor.close()

if __name__ == "__main__":
    getQuery()