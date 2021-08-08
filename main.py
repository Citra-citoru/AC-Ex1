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
        sql1 = "SELECT TOP 10 [partner Name]= pa.name,[product Name]= pr.code,[Avg Loan Size]= ISNULL(AVG(l.TotalDisbursementAmount),0),[Total of Loans]= ISNULL(SUM(l.loanNumber),0),[Total Amount of Disbursed]= ISNULL(SUM(l.TotalDisbursementAmount),0),[Total of Unique Borrower]= ISNULL(COUNT(DISTINCT app.borrowerId),0) FROM [dbo].Borrowers b INNER JOIN [dbo].Applications app on b.id = app.borrowerId INNER JOIN [dbo].Loans l on app.id = l.applicationId INNER JOIN [dbo].products pr on pr.id = app.productId INNER JOIN [dbo].partners pa on pa.id = pr.partnerId GROUP BY pa.name,pr.code ORDER BY ISNULL(SUM(l.TotalDisbursementAmount),0) DESC"
        sql2 = "SELECT TOP 10 [partner Name]= pa.name,[partnerBorrowerId]= b.partnerBorrowerId,[Total of Loans]= ISNULL(SUM(l.loanNumber),0),[Total Amount of Disbursed]= ISNULL(SUM(l.TotalDisbursementAmount),0),[Avg Loan Size]= ISNULL(AVG(l.TotalDisbursementAmount),0) FROM [dbo].Borrowers b INNER JOIN [dbo].Applications app on b.id = app.borrowerId INNER JOIN [dbo].Loans l on app.id = l.applicationId INNER JOIN [dbo].products pr on pr.id = app.productId INNER JOIN [dbo].partners pa on pa.id = pr.partnerId GROUP BY pa.name,b.partnerBorrowerId ORDER BY ISNULL(SUM(l.TotalDisbursementAmount),0) DESC"
        
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