import io

import fitz
from PIL import Image
import time

import easyocr


def image_to_text():
    reader = easyocr.Reader(['en'])
    start_time = time.time()
    result = reader.readtext(
        '/Users/maheshvakkund/Documents/PYTHON/spark-mra-poc/output/mahesh.jpeg', detail=0)
    print("--- %s seconds ---" % (time.time() - start_time))
    print(result)


if __name__ == '__main__':

    pdf_file = fitz.open("/Users/maheshvakkund/Documents/PYTHON/spark-mra-poc/mra-resources/file_example_TIFF_1MB.pdf")
    for page_index in range(len(pdf_file)):
        # get the page itself
        page = pdf_file[page_index]
        # get image list
        image_list = page.get_images()
        # printing number of images found in this page
        if image_list:
            print(f"[+] Found a total of {len(image_list)} images in page {page_index}")
        else:
            print("[!] No images found on page", page_index)
        for image_index, img in enumerate(image_list, start=1):
            # get the XREF of the image
            xref = img[0]
            # extract the image bytes
            base_image = pdf_file.extract_image(xref)
            image_bytes = base_image["image"]
            # get the image extension
            image_ext = base_image["ext"]
            # load it to PIL
            image = Image.open(io.BytesIO(image_bytes))
            # save it to local disk
            image.save('/Users/maheshvakkund/Documents/PYTHON/spark-mra-poc/output/mahesh.jpeg')
            image_to_text()
