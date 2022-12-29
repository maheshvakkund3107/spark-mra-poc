import time
import easyocr

if __name__ == '__main__':
    reader = easyocr.Reader(['en'])
    start_time = time.time()
    result = reader.readtext(
        '/Users/maheshvakkund/Documents/PYTHON/spark-mra-poc/mra-resources/file_example_TIFF_1MB.tiff', detail=0)
    print("--- %s seconds ---" % (time.time() - start_time))
    print(result)
