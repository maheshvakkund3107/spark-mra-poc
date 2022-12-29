import random
import time

import pdfkit
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from tika import parser

from fpdf import FPDF


def udf_bin_to_text_using_tika(data):
    parsed = parser.from_buffer(data)
    return extract_address(parsed['content'])


def extract_address(text):
    remove = ['DP ID', 'Client ID', 'Trading ID', 'PAN No']
    head = 'TRANSACTION WITH HOLDING STATEMENT'
    tail = 'Account Status'
    address = 'ADDRESS'
    lines = text.split("\n")
    lines = [i for i in lines if i]
    head_index = lines.index(head)
    lines = lines[head_index + 1:]
    for line in lines:
        if line.__contains__(tail):
            tail_index = lines.index(line)
    lines = lines[:tail_index]
    for line in lines:
        flag = 0
        for word in remove:
            if line.__contains__(word):
                flag = 1
                address = address + '\n' + line[:line.find(word)]
                break

        if line.lower() != 'to,' and line != '' and line != ' ' and flag != 1:
            address = address + '\n' + line
    print(address)
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=15)
    pdf.multi_cell(200, 10, txt=address, align='L')
    pdf.output(f'/Users/maheshvakkund/Documents/PYTHON/spark-mra-poc/output/address{random.randint(10, 2000)}.pdf')
    return address


def extract_info(text):
    headers = ['Name', 'PAN', 'Date']
    headers_str = ' '.join(headers)
    result = {}
    for header in headers:
        result[header] = []
    lines = text.split("\n")
    lines = [i for i in lines if i]
    for line in lines:
        if line == headers_str:
            index = lines.index(line)
    result['Date'] = lines[index + 1][-10:]
    result['PAN'] = lines[index + 1][-21:-11]
    result['Name'] = lines[index + 1][:-22]
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=15)
    pdf.cell(200, 10, txt=f"DATE :{result['Date']} PAN:{result['PAN']} NAME:{result['Name']}",
             ln=2, align='L')
    pdf.output(f"/Users/maheshvakkund/Documents/PYTHON/spark-mra-poc/output/{result['Date']}-{result['PAN']}.pdf")
    return f"DATE :{result['Date']} PAN:{result['PAN']} NAME:{result['Name']}"


if __name__ == '__main__':
    sparkSession = SparkSession. \
        builder. \
        appName("Spark-Tika-Pdf"). \
        master("local[*]"). \
        getOrCreate()
    file_path = [
        "/Users/maheshvakkund/Documents/PYTHON/spark-mra-poc/mra-resources/monthly-transaction*.pdf"]
    start = time.time()
    read_data_df = sparkSession.read.format("binaryFile").load(file_path)
    read_data_df.printSchema()
    udf_bin_to_text_using_tika_v = udf(lambda z: udf_bin_to_text_using_tika(z), StringType())
    final_df = read_data_df. \
        select("content"). \
        withColumn("text_data", udf_bin_to_text_using_tika_v("content"))
    end = time.time()
    print("The time of execution of above program is :",
          (end - start) * 10 ** 3, "ms")
    final_df.select("text_data").show(truncate=False)
    final_df = final_df.select("text_data")
