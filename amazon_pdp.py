from AmazonPDP_Mandatory import automate_Amazon_PDP_Mandatory 
from AmazonPDP_NonMandatory import Amazon_PDP_Non_Mandatory
import pandas as pd
import time
import glob2
import os


BASE_DIR = os.path.dirname(os.path.abspath('__file__'))

def amazon_pdp(batch_name, ip_address):
    scraped_df = pd.DataFrame()
    error_df = pd.DataFrame()

    scraped_data_mandatory, error_data_mandatory = automate_Amazon_PDP_Mandatory(batch_name, ip_address)

    scraped_data_non_mandatory, error_data_non_mandatory = Amazon_PDP_Non_Mandatory(batch_name,error_data_mandatory, ip_address)

    final_error_df = error_data_non_mandatory.copy()
    final_scraped_data = pd.concat([scraped_data_mandatory,scraped_data_non_mandatory])
    final_scraped_data.drop_duplicates(subset="Product ID", keep="first", inplace=True)
    print("==============",final_error_df)
    exception_dir = os.path.join(BASE_DIR + '\\excep')
    output_dir = os.path.join(BASE_DIR + '\\output')
    final_error_df.to_excel(r"{0}\\ERROR_{1}.xlsx".format(exception_dir , batch_name))
    final_scraped_data.to_excel(r"{0}\\{1}.xlsx".format(output_dir , batch_name), index=False)
    
    # final_error_df.to_excel(r"D:\Abhi\Web Scraping\All PdP Files\amazonPDP_env\amazonPDP\amazonPDP\spiders\excep\ERROR_{0}.xlsx".format(batch_name))
    # final_scraped_data.to_excel(r"D:\Abhi\Web Scraping\All PdP Files\amazonPDP_env\amazonPDP\amazonPDP\spiders\output\{0}.xlsx".format(batch_name), index=False)
    return "Success"

def automate_amazon_pdp(folder_path):
    """
    folder_path --> This is the path of batches folder 
    (Note: You have to use forward single slash in folder path)
    """
    excel_files = glob2.glob(folder_path + '/*.xlsx')
    ip = ['107.186.52.30','107.186.52.62','107.186.52.94','107.186.52.126','107.186.52.158']
    count = 0
    for filename in excel_files:
        print(filename)
        count += 1
        if count>4:
            count = 0
        batch0 = filename.split("\\")[-1]
        batch = batch0.replace(".xlsx","")
        amazon_pdp(batch, ip[count])
        time.sleep(120)
input_path = os.path.join(BASE_DIR + '\\input')
batch_path = r"{0}".format(input_path)
automate_amazon_pdp(batch_path)