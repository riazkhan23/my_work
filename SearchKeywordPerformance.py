'''
This program reads the file from input parameters and finds the which search engine and which key word
the users are using while buying online products.

Created by : Riaz Khan
Last Updated : 04/06/2022

Usage :  python3 SearchKeywordPerformance.py <input file name>
Example : python3 SearchKeywordPerformance.py data.tsv
'''

import sys
from urllib.parse import urlparse,parse_qs
import logging
from datetime import date,datetime
import smart_open
import re
import boto3
now = datetime.now()
today_date = str(now.strftime("%Y-%m-%d"))
date_time=str(now.strftime("%Y_%m_%d_%H_%M_%S"))
s3_bucket_name='rapsui-dev-data-in'
s3 = boto3.resource('s3')

log_file_name='logs/SearchKeywordPerformance_'+date_time+'.log'
logging.basicConfig(filename=log_file_name,format='%(asctime)s %(levelname)-10s %(message)s',filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class parse_data () :
    def __init__(self, row):
        self.rec = row
    def get_referrer(self):
        return self.rec.split('\t')[11].strip('\n').strip(' ')
    def get_page_url(self):
        return self.rec.split('\t')[9].strip('\n').strip(' ')
    def get_ip_address(self):
        return self.rec.split('\t')[3].strip(' ')
    def get_product_list(self):
        return self.rec.split('\t')[10].split(',')
    def get_event_list(self):
        return self.rec.split('\t')[4].split(',')


if __name__ == "__main__":
    print("Program started...")
    logger.info("Program started")

    if len(sys.argv)-1 != 1:
        print('Pass file name/right number of arguments to the program. Exiting ....')
        logger.error("Right arguments didnt pass")
        sys.exit()
    else:
        input_filename= sys.argv[1]
        logger.info("Argumnets read successfully")

    try :
        s3_file_path='s3://'+s3_bucket_name+'/poc/'+input_filename
        x= smart_open.smart_open(s3_file_path)
        lines = x.readlines()[1:] # This command to remove header
        logger.info("Read the input file successfully")
        lines = sorted(lines, key=lambda row: (row.decode('utf-8').replace('\n','').split("\t")[3], \
        datetime.strptime(row.decode('utf-8').replace('\n','').split("\t")[1], '%Y-%m-%d %H:%M:%S') ) )
        logger.info("Records sorted on ip_address and date_time successfully")
        prev_ip_address = None
        prev_page_url = None
        total_search_engine_revenue_dic = {}
        logger.info("Processing of each record started")
        for line in lines:
            line=line.decode('utf-8').replace('\n','')
            record=parse_data(line)
            referrer=record.get_referrer()
            page_url=record.get_page_url()
            ip_address=record.get_ip_address()
            product_list=record.get_product_list()
            event_list=record.get_event_list()
            search_engine = urlparse(referrer).netloc
            search_engine=search_engine[search_engine.index('.')+1:]
            parsed_url = urlparse(referrer)
            pars_values=parse_qs(parsed_url.query)
            referrer=re.sub('[^A-Za-z0-9]+', '', referrer)
            page_url=re.sub('[^A-Za-z0-9]+', '', page_url)
            # Find if the website is search engine.
            search_key=pars_values.get('q',pars_values.get('p',pars_values.get('text',pars_values.\
            get('query','Not a Search Engine'))))
            if search_key=='Not a Search Engine':
                search_engine='Not a Search Engine'

            if (prev_ip_address == None or prev_ip_address!= ip_address ) and search_key != 'Not a Search Engine':
                search_key=search_key[0]

            elif (prev_ip_address == None or prev_ip_address!= ip_address ) and search_key == 'Not a Search Engine':
                search_engine='Not a Search Engine'
                search_key='Not a Search Engine'

            elif ip_address==prev_ip_address and referrer==prev_page_url \
            and prev_search_engine != 'Not a Search Engine' and search_key=='Not a Search Engine':
                search_key=prev_search_key
                search_engine=prev_search_engine

            elif ip_address==prev_ip_address and referrer != prev_page_url \
            and search_key !='Not a Search Engine':
                search_key=search_key[0]

            elif ip_address==prev_ip_address and referrer != prev_page_url \
            and search_key =='Not a Search Engine':
                search_engine='Not a Search Engine'
                search_key='Not a Search Engine'

            # The below condition checks if the transaction happend and if its seach engine transaction
            if '1' in event_list and search_engine != 'Not a Search Engine':
                revenue = [ (0 if (price.split(';')[3].strip(' ') =='' or price.split(';')[2] !='1') \
                else int(price.split(';')[3])) if len(price.split(';')) > 3 else 0 for price in product_list  ]
                search_engine_key_word=search_engine+'|'+search_key
                if len(revenue) != 0:
                    if search_engine_key_word not in total_search_engine_revenue_dic.keys():
                        total_search_engine_revenue_dic[search_engine_key_word]=sum(revenue)
                    else:
                        revenue=total_search_engine_revenue_dic.get(search_engine_key_word,0)+sum(revenue)
                        total_search_engine_revenue_dic.update({search_engine_key_word:revenue})
            # stores data to temporary varaiables to compare next record.
            prev_search_key=search_key
            prev_search_engine=search_engine
            prev_ip_address=ip_address
            prev_page_url=page_url

        logger.info("Processing of each record completed successfully")
        #sort the dictionaly elements on revenue.
        total_search_engine_revenue_dic={k: v for k, v in sorted(total_search_engine_revenue_dic.items()\
        , key=lambda item: item[1], reverse=True)}
        logger.info("Final output sorted successfully on Revenue")

        output_header = "Search Engine Domain"+'\t'+"Search Keyword"+'\t'+"Revenue"
        output_data=output_header
        # Its creating the textual data for s3 load
        for search_engine_key_val in total_search_engine_revenue_dic:
            search_engine=search_engine_key_val.split('|')[0]
            search_key=search_engine_key_val.split('|')[1]
            total_rev=total_search_engine_revenue_dic[search_engine_key_val]
            output_data=output_data+'\n'+search_engine+'\t'+search_key+'\t'+str(total_rev)


        # Writes data to s3
        object = s3.Object(s3_bucket_name, 'poc/'+today_date+"_SearchKeywordPerformance.tab")
        object.put(Body=output_data)

        logger.info("Data written to a file - "+today_date+"_SearchKeywordPerformance.tab")
        #print(total_search_engine_revenue_dic)
        logger.info("Program completed successfully!")
        print("Program completed successfully!")
    except Exception as ERROR:
        logger.error(ERROR)
        print("Error occuried in the program. Please check log file for more details.",ERROR)
        raise ValueError(ERROR)
