#For loop automation

# Packages required to run the script
import os
from urllib.parse import urlparse
import pandas as pd
import scrapy as sp
import re
import datetime
import random
from scrapy.crawler import CrawlerProcess
from fake_useragent import UserAgent
import time
import sys 
import random

# ip = ['107.186.52.30','107.186.52.62','107.186.52.94','107.186.52.126','107.186.52.158']


BASE_DIR = os.path.dirname(os.path.abspath('__file__'))
no_filtered_dir = os.path.join(BASE_DIR + '\\NoFilteredData')
def Amazon_PDP_Mandatory(batch_name_or_df, ip_address):
    ERROR_IDS = []
    mandatory =[]
    all_data_list =[]

    # Respective files required to run the script
    if type(batch_name_or_df)==str:
        SCRAPED_DATAFILE = batch_name_or_df #This will save the file from feeds of scrapy
        # INPUT_BATCH_FILE = r"D:\Abhi\Web Scraping\All PdP Files\amazonPDP_env\amazonPDP\amazonPDP\spiders\input\{}.xlsx".format(SCRAPED_DATAFILE)
        Input_path = os.path.join(BASE_DIR + "\\input")
        INPUT_BATCH_FILE = r'{0}\\{1}.xlsx'.format(Input_path, SCRAPED_DATAFILE)
        df = pd.read_excel(INPUT_BATCH_FILE)
        ProductID_List = list(df['ID'])
    else:
        SCRAPED_DATAFILE = batch_name_or_df.copy()
        ProductID_List = list(SCRAPED_DATAFILE["Item Code"])
    
    ERROR_FILE =str("ERROR IDS_" + SCRAPED_DATAFILE + '.xlsx')#This will have the error Id's both mandatory id's and exception Id's
    
    FINALOUTPUT_FILE = f"{SCRAPED_DATAFILE}_Out.xlsx" #This will save the data after all the PostProcessing
    # XPATH_FILENAME = r"D:\Abhi\Web Scraping\All PdP Files\amazonPDP_env\amazonPDP\amazonPDP\spiders\Amazon xpaths file.xlsx"
    XPATH_FILENAME = r"Amazon xpaths file.xlsx"
    if "twisted.internet.reactor" in sys.modules:
        print("Twisted Reactor Found")
        del sys.modules["twisted.internet.reactor"]
    else:
        print("No twisted reactor found")
    # Input batch file with sheet name as "amazon"

    # Reading an XPATHS file
    xpath_file = pd.read_excel(XPATH_FILENAME, sheet_name="amazon")

    # Elements , Xpaths name as dictionary
    xpaths = dict(zip(xpath_file['elements'], xpath_file['xpaths']))


    # Type of xpath as GET , GETALL if xpaths file
    xpaths_type = dict(zip(xpath_file['elements'], xpath_file['type']))

    # User Agents list to the user agent for every request to the scrapy
    user_agent_list = []

    # List of fake user agents
    for i in range(5):
        ua = UserAgent(browsers=['edge', 'chrome'])
        user_agent_list.append(ua.random)


    class AmazonPdpSpider(sp.Spider):

        name = 'amazon_pdp'
        # Domain name of the ecommerce platform
        allowed_domains = ['amazon.com']

        # Custom setting for the AmazonPDP Spider
        custom_settings = {
            'DOWNLOAD_FAIL_ON_DATALOSS': False,
            'CONCURRENT_ITEMS': 1,
            'DOWNLOAD_TIMEOUT': 30,
            'CONCURRENT_REQUESTS': 32,
            'CONCURRENT_REQUESTS_PER_DOMAIN': 20,
            'TWISTED_REACTOR': 'twisted.internet.selectreactor.SelectReactor',
            'ROBOTSTXT_OBEY': True,
            # 'DOWNLOAD_MIDDLEWARES':{'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware':110,'AmazonPdpSpider.smartproxy_auth.ProxyMiddleware':100},
            # 'SMARTPROXY_USER':' http://user-Dhiomics',
            # 'SMARTPROXY_PASSWORD':'Dhiomics123',
            # 'SMARTPROXY_ENDPOINT':'in.smartproxy.com',
            # 'SMARTPROXY_PORT':10000
        }

        # Starting a request to the scrapy
        def start_requests(self):
            for asin in ProductID_List:
                # Formating the producturl with asin number
                ProductURL = '''https://www.amazon.in/dp/{0}?th=1&psc=1'''.format(
                    str(asin))
                # ip_rand = random.choice(ip)
                yield sp.Request(url=ProductURL, callback=self.parse, meta={"ASIN": asin,
                                                                             'proxy':'http://customer-adaip:ADAips123@dc.us-pr.oxylabs.io:30000', 
                                                                             'handle_httpstatus_all': True}, headers={"USER_AGENT": user_agent_list[random.randint(0, len(user_agent_list)-1)]})

        # Response Data function
        def parse(self, response):
            URL = response.url
            Output_Data = {}
            asin = re.findall('B0.{8}', URL)[0]
            print("*************Response*****************", response.status)
            if(response.status == 503 or response.status == 404 or response.status == 403 or
               response.status == 407 or response.status == 466):
                ERROR_IDS.append(asin)
                # print("HHHHHHFDDDDDDDDDDDDDDDDDDDDDDDDDDSAAAAAAAAAAAAAAAAAAA",asin)
            today = datetime.date.today()
            year, wk_num, day_of_wk = today.isocalendar()
            dtime = datetime.datetime.now()
            dt = dtime.strftime(r'%Y-%m-%d')

            # Manual added feilds to the final output file
            Output_Data['Date'] = dt
            Output_Data['Week'] = wk_num

            # Extracting the marketplace from the base url
            Output_Data['MarketPlace'] = urlparse(response.url).netloc.replace(
                'www.', '').replace('.com', '').replace(".in", '').title()
            Output_Data['Product ID'] = asin
            Output_Data['Product URL'] = URL
            try:
                title_out = response.xpath("//span[@id='productTitle']/text()").get().strip()
                Output_Data['Title'] = title_out
                brand = str(title_out).split(' ')[0]
                Output_Data['Brand'] = brand
                # Output_Data['Brand'] = str(title_out.split("")[0])
            except:
                # Output_Data['Brand'] = 'na'
                Output_Data['Title'] = 'na'

            try:
                Output_Data['Category'] = response.xpath("(//*[@class='a-link-normal a-color-tertiary'])[3]/text()").get().strip()
            except:
                Output_Data['Category'] = 'na'
            try:
                Output_Data['In Stock'] = response.xpath("//*[@id='availability']/span/text()").get().strip()
            except:
                Output_Data['In Stock'] = 'na'
            # Iterating the key of an XPATHS and getting the data
            try:
                Output_Data['Sub-Category'] = response.xpath("(//*[@class='a-link-normal a-color-tertiary'])[4]/text()").get().strip()
            except:
                Output_Data['Sub-Category'] = 'na'

            try:
                try:
                    Output_Data['Image URL'] = response.xpath("//*[@id='imgTagWrapperId']/img/@src").get().strip()
                except:
                    Output_Data['Image URL'] = response.xpath('//*[@id="anonCarousel1"]/ol/li[1]/div/span/img/@src').get().strip()
            except:
                Output_Data['Image URL'] = 'na'
            try:
                img_len = response.xpath("//span[@class='a-list-item']/span/span/span/img/@src").getall()
                print("))))))))))))))))))", img_len , len(img_len))
                Output_Data['No. of Images'] = len(img_len)
            except:
                Output_Data['No. of Images'] = 'na'
            for key in xpaths.keys():

                # Getting the data for single repeated element with get() functionality of a scrapy
                if (xpaths_type[key] == 'get'):
                    try:
                        Output_Data[key] = response.xpath(str(
                            xpaths[key])).get().strip() or 'na'

                    except:
                        # Except the value will be 'na'
                        Output_Data[key] = 'na'

                # Getting the data for multiple repeated elements with getall() functionality of a scrapy
                elif (xpaths_type[key] == 'getall'):
                    try:
                        Output_Data[key] = response.xpath(
                            xpaths[key]).getall() or 'na'

                        # If Product Details is not scraped we are moving that ASIN to error file
                        # Cleaning the data of Product Details in key value pair
                        if (key == 'Product Details' and Output_Data[key] != 'na'):
                            Output_Data[key] = [
                                value for value in Output_Data[key] if value.strip()]

                            Output_Data[key] = [value.replace("/n", "").replace("\u200f", "").replace(
                                "\u200e", "").replace(":", "") for value in Output_Data[key]]

                            Output_Data[key] = {Output_Data[key][value].strip(): Output_Data[key][value + 1]
                                                for value in range(0, len(Output_Data[key]), 2)}
                        # Images count
                        # if (key == 'No. of Images'):
                        #     try:
                        #         Output_Data[key] = len(
                        #             response.xpath(xpaths[key]).getall())
                        #     except:
                        #         Output_Data[key] = 'na'

                        # Total sizes
                        if (key == 'Total Sizes'):
                            try:
                                Output_Data[key] = response.xpath(xpaths[key]).getall() or response.xpath(
                                    "//span[@class = 'twister-dropdown-highlight transparentTwisterDropdownBorder']/span/select/option/text()").getall() or response.xpath(
                                     "//select[@id='native_dropdown_selected_size_name']/option[position() > 1]/text()").getall()
                                     
                            except:
                                Output_Data[key] = '[]'

                            try:
                                Output_Data['No. of Sizes'] = len(response.xpath(xpaths[key]).getall()) or len(response.xpath(
                                    "//span[@class = 'twister-dropdown-highlight transparentTwisterDropdownBorder']/span/select/option/text()").getall())
                            except:
                                Output_Data[key] = 0

                        # Colors
                        if (key == 'Color'):
                            try:
                                # Output_Data[key] = response.xpath(xpaths[key]).getall() or 'na'
                                Output_Data['No. of Colors'] = len(response.xpath(xpaths[key]).getall()) or len(response.xpath(
                                    "//div[@class='a-section a-spacing-none swatch-image-container']/img/@alt").getall()) or 'na'
                            except:
                                Output_Data[key] = 'na'

                    except:
                        Output_Data[key] = 'na'
                else:
                    Output_Data[key] = 'NONE'
            # all_data_list.append(Output_Data)
            if (Output_Data['Title'] == 'na' or Output_Data['Product Details'] == 'na' or   Output_Data['In Stock'] == 'Currently unavailable.' or Output_Data['In Stock'] == 'na'
            or Output_Data['Image URL']=='na' or Output_Data['Selling Price']=='na' or Output_Data['MRP']=='na'
            or Output_Data['COD']=='na' or Output_Data['Count of Ratings']=='na' or Output_Data['Product Rating']=='na'):
                ERROR_IDS.append(asin)
                # print("Trying IDS", len(ERROR_IDS))
            else:
                all_data_list.append(Output_Data)


    # Crawler to run the spider file
    process = CrawlerProcess(settings={'LOG_LEVEL': 'DEBUG',
                                    'USER_AGENT': user_agent_list[random.randint(0, len(user_agent_list)-1)],
                                    # 'FEEDS': {f'{SCRAPED_DATAFILE}.csv': {'format': 'csv', 'overwrite': True}}
                                    })
    process.crawl(AmazonPdpSpider)
    process.start()


    # Reading an output file for the postprocessing
    # OutputDf_Scraped = pd.read_csv(f'{SCRAPED_DATAFILE}.csv')
    OutputDf_Scraped = pd.DataFrame(all_data_list)
    print("____________________",len(OutputDf_Scraped))
    # Creating an error file
    # print("Errror IDS", list(set(ERROR_IDS)))
    error_df = pd.DataFrame({"Item Code": list(set(ERROR_IDS))})

    # Checing if Error ID file have no ids , then we are removing the file or else we are removing the duplicate IDs
    if (len(ERROR_IDS) > 0):
        try:
            data = pd.read_excel(ERROR_FILE)
            data.sort_values("Item Code", inplace=True)
            data.drop_duplicates(subset="Item Code", keep=False, inplace=True)
        except:
            pass
    else:
        try:
            os.remove(ERROR_FILE)
        except:
            pass
    # Post Prossesing the output file to filter the data as per the requirements
    def Postprocessing(df):
        try:
            try:
                old_df = pd.read_excel(r"{}\\{}_MandatoryData.xlsx".format(no_filtered_dir,batch_name_or_df))
                new_df = pd.concat([old_df, df])
                new_df.drop_duplicates(subset="Product ID",
                        keep=False, inplace=True)
                new_df.to_excel(r"{}\\{}_MandatoryData.xlsx".format(no_filtered_dir,batch_name_or_df), index=False)
                
            except:

                df.to_excel(r"{}\\{}_MandatoryData.xlsx".format(no_filtered_dir,batch_name_or_df), index=False)
            df.insert(26, "No of Available Sizes", "")
            # df.insert(27, "Non-Available Sizes", "")
            df.insert(28, "No of Non-Available Sizes", "")
            df.insert(29, "Count of Reviews", "")
            df.insert(38, "5 STAR", "0")
            df.insert(39, "4 STAR", "0")
            df.insert(40, "3 STAR", "0")
            df.insert(41, "2 STAR", "0")
            df.insert(42, "1 STAR", "0")

            for row in list(df.index):
                # For all Count of review stars
                # print("---------------",df['Stars'][row])
                Main_Starlist = str(df['Stars'][row]).replace("[","").replace("]","").split(",")
                # print("==============",Main_Starlist)

                # Replacing unwanted data of Product Rating
                df['Product Rating'][row] = df['Product Rating'][row].replace(
                    'out of 5 stars', '')

                # Replacing unwanted data of Count of Ratings
                df['Count of Ratings'][row] = df['Count of Ratings'][row].replace(
                    'ratings', '').replace("rating", "")

                # Replacing unwanted data of Selling Price
                try:
                    if (df['Selling Price'][row] != 'na'):
                        df['Selling Price'][row] = df['Selling Price'][row].replace(
                            '₹', '')
                    else:
                        df['Selling Price'][row] = ''
                except:
                    pass

                # Replacing unwanted data of MRP
                try:
                    if (df['MRP'][row] != 'na'):
                        df['MRP'][row] = df['MRP'][row].replace('₹', '')
                    else:
                        df['MRP'][row] = ''
                except:
                    pass

                # Replacing unwanted data of Discount
                try:
                    if (df['Discount %'][row] != 'na'):
                        df['Discount %'][row] = df['Discount %'][row].replace(
                            '-', '').replace("%", '')
                    else:
                        df['Discount %'][row] = ''
                except:
                    pass

                # Replacing unwanted data of Number of Questions
                try:
                    if (df['Ques'][row] != 'na'):
                        df['Ques'][row] = df['Ques'][row].replace(
                            'answered questions', '')
                    else:
                        df['Ques'][row] = ''
                except:
                    pass

                # Seller URL
                try:
                    if (df['Seller URL'][row] != 'na'):
                        df['Seller URL'][row] = "https://www.amazon.in/" + \
                            str(df['Seller URL'][row])
                except:
                    pass

                # Total Sizes
                try:
                    if (df['Total Sizes'][row] != 'na'):
                        df['Total Sizes'][row] = [item for item in str(
                            df['Total Sizes'][row]).split(",")]
                        del df['Total Sizes'][row][0]
                except:
                    pass

                # Total Number of sizes
                try:
                    if (df['No. of Sizes'][row] != 0):
                        df['No. of Sizes'][row] = df['No. of Sizes'][row]-1
                except:
                    pass

                # Offers
                try:
                    if (df['Offers'][row] != 'na'):
                        df['Offers'][row] = df['Offers'][row].split(",")
                        ResultOffers = [
                            ele for ele in df['Offers'][row] if ele.strip()]
                        df['Offers'][row] = ResultOffers
                    else:
                        df['Offers'][row] = '[]'
                except:
                    pass

                # Extracting the BestSeller Rank
                try:
                    if (df['Bestseller Rank'][row]):
                        # print("Besteseller",df['Rank Detail'][row])
                        a = df['Bestseller Rank'][row]
                        b = re.findall(r"[0-9,]{3,15}", str(a))
                        c = str(b[0]).replace(",","")
                        df['Bestseller Rank'][row] = c
                        cleaned_list = [item.strip('() ') for item in df['Rank Detail'][row] if '(' in item or ')' in item or ' ' in item]
                        # df['Rank Detail'][row] = str(df['Rank Detail'][row]).replace(
                        #     ",", "").strip()
                        df['Rank Detail'][row] = cleaned_list
                    else:
                        df['Bestseller Rank'][row] = 'na'
                        df['Rank Detail'][row] = 'na'

                except:
                    pass

                # Customer Reviews for 5 , 4 , 3 , 2 , 1 Stars
                try:
                    if (Main_Starlist[0] != 'na'):
                        out = [int(values.split(' ')[-2]) for values in Main_Starlist]
                        values = list(set([row for row in range(1, 6)]) - set(out))
                        for value in values:
                            Main_Starlist.append(
                                '0% of reviews have {} stars'.format(value))
                        df['5 STAR'][row] = [val for val in list(Main_Starlist) if (
                            int(val.split(' ')[-2]) == 5)][0].replace('% of reviews have 5 stars', '')
                        df['4 STAR'][row] = [val for val in list(Main_Starlist) if (
                            int(val.split(' ')[-2]) == 4)][0].replace('% of reviews have 4 stars', '')
                        df['3 STAR'][row] = [val for val in list(Main_Starlist) if (
                            int(val.split(' ')[-2]) == 3)][0].replace('% of reviews have 3 stars', '')
                        df['2 STAR'][row] = [val for val in list(Main_Starlist) if (
                            int(val.split(' ')[-2]) == 2)][0].replace('% of reviews have 2 stars', '')
                        df['1 STAR'][row] = [val for val in list(Main_Starlist) if (
                            int(val.split(' ')[-2]) == 1)][0].replace('% of reviews have 1 stars', '')
                except:
                    pass

                # No. of Colors
                if (df['No. of Colors'][row] != 'na'):
                    df['No. of Colors'][row] = int(df['No. of Colors'][row])

                # if (df['Total Sizes'][row] == None):
                #     df['Total Sizes'][row] = 'na'

                # If Buy Now exists "YES" if not "NO"
                if (df['Buy Now'][row] == 'Buy Now'):
                    df['Buy Now'][row] = 'Yes'
                else:
                    df['Buy Now'][row] = 'No'

                # If Cash on Delivery exists "YES" if not "NO"
                if (df['COD'][row] == 'na'):
                    df['COD'][row] = 'No'
                else:
                    df['COD'][row] = 'Yes'

                # If In stock exists "YES" if not "NO"
                if (df['In Stock'][row] != 'Currently unavailable.' ):
                    df['In Stock'][row] = 'In Stock'
                else:
                    df['In Stock'][row] = 'Out Of Stock'

                # Total Sizes
                if df['Total Sizes'][row] != 'na':
                    df['Total Sizes'][row] = df['Total Sizes'][row]

                # List of Colors
                if df['Color'][row] != 'na':
                    df['Color'][row] = str(df['Color'][row]).split(',')
                    df['Color'][row] = [ele for ele in df['Color'][row] if ele.strip()]

                # No. of offers
                df['No. of Offers'][row] = sum(
                    [int(i)for i in ''.join(df['No. of Offers'][row]) if i.isdigit()])
                
                if(str(df['Title'][row])=='na' and str(df['Selling Price'][row])=='na' and str(df['MRP'][row])=='na'
                    and str(df['Discount %'][row]) == 'na' and str(df['Count of Ratings'][row]) == 'na'):
                        # print("==before==",df['In Stock'][row])
                        df['In Stock'][row] = 'Out Of Stock'

            # Droping the Stars column because we are making a seperating column for each star 5 , 4 , 3 , 2 , 1
            df.drop(['Stars'], axis="columns", inplace=True)

            df.drop_duplicates(subset="Product ID",
                        keep=False, inplace=True)
            df = df.replace('na','')
            # Returning an updated Dataframes
            return df
        except:
            pass


    # Post Processing function to filter the DataFrame
    scraped_df = Postprocessing(OutputDf_Scraped)

    print("====ERROR DF=======", error_df)
    return scraped_df, error_df

def automate_Amazon_PDP_Mandatory(batch_name, ip_address):
    scraped_df = pd.DataFrame()
    error_df = pd.DataFrame()
    time.sleep(3)
    print(f"Batch Name: ",{batch_name})
    time.sleep(3)
    s_df, e_df = Amazon_PDP_Mandatory(batch_name, ip_address)
    error_df = e_df.copy()
    try:

        scraped_df = s_df.copy()
    except:
        pass
    for i in range(1,3):
        time.sleep(3)
        print(f"error file {i}")
        time.sleep(3)
        s_df, e_df = Amazon_PDP_Mandatory(error_df, ip_address)
        error_df = e_df.copy()
        scraped_df = pd.concat([scraped_df,s_df])
    time.sleep(5)
    print("########## Mandatory SUCCESS ##########")
    return scraped_df, error_df

