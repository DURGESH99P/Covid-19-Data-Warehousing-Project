<h1 style="text-align:center;font-family:'Times New Roman';color:darkblue;font-weight:700;font-size:40px"> Covid-19 Data Engineering Project </h1>

<hr style="border: 0.001px solid black">

# The Architecture  👇

![DE project architecrure diagram](https://github.com/DURGESH99P/DataEngineering-Projects/assets/94096617/0af7fe22-a250-41f3-a786-4257122964bc)


# Create an S3 bucket for the project 

![1](https://github.com/DURGESH99P/DataEngineering-Projects/assets/94096617/5c7b14ed-cf9c-4cfb-8221-6f0f644b60c3)


# Upload the Data to the bucket
### **data was taken from covid-19 data lake:** https://dj2taa9i652rf.cloudfront.net/

![2.png](attachment:2.png)

---

# Create IAM Role for Glue crawler

![iam8.png](attachment:iam8.png)

## **Below 👇image demonstrates the policies attached to the role**

![iam9.png](attachment:iam9.png)

---

# Create Glue Crawlers for each dataset

## 1. Set crawlers Properties

![3.png](attachment:3.png)

## 2. Add Data source 

![4.png](attachment:4.png)

## Below 👇image demonstrates the selection of datasource for the dataset enigma-jhud
### *Note: choose the folder of the dataset not the dataset file*

![5.png](attachment:5.png)

## 3. Choose the option crawl all the sub-folders in the Subsequent crawler runs

![6.png](attachment:6.png)

## 4. Configure the security settings and select IAM role you have created for Glue crawler

![7.png](attachment:7.png)

## Below image 👇 shows the IAM role attached

![10.png](attachment:10.png)

## 5. Choose option "Add database" to create database

![11.png](attachment:11.png)

## Give the database name and choose option "Create database" 

![12.png](attachment:12.png)

## 6. Choose option next and Create the crawler

![13.png](attachment:13.png)

## 7. After successful creation run the crawler

![14.png](attachment:14.png)

## Similarly create crawlers for all the datasets and run them

![all%20crawlers.png](attachment:all%20crawlers.png)

---

# Query datasets in athena
## • Go to Manage settings in query editor and add the location of S3 bucket you want to store the query results
###  *Note: You have to create a bucket to store the query result data as shown in the image and add create a folder named "output" for the query responses and "packages" for storing redshift-connector .whl library file later in it*

![Athena1.png](attachment:Athena1.png)

---

# Using boto3 to perform task through IDE

<span style ="font-weight:400;font-size:17px">
<body>
    <p>Boto3 is a Python package that allows you to interact with Amazon Web Services (AWS). AWS is a cloud platform that provides various services such as storage, computing, monitoring, etc. Boto3 lets you use Python code to access and manage these services. You can install Boto3 from pip installer, and configure your credentials and region. Boto3 supports many AWS services and has a comprehensive documentation.</p>
  </body>
</span>



```python
import boto3
import pandas as pd
from io import StringIO
```


```python
AWS_ACCESS_KEY = "Put your Iam User Access key here"
AWS_SECRET_KEY = "Put your Iam User secret Access key here"
# If no IAM user is created create an IAM user with Administrative access
# and get its access key ID & seccret access key
AWS_REGION = "ap-south-1"
SCHEMA_NAME = "covid-19"
S3_STAGING_DIR = "s3://durgeshp-test-bucket/output"
S3_BUCKET_NAME = "durgeshp-test-bucket"
S3_OUTPUT_DIRECTORY = "output"
```

## Create athena client


```python
athena_client = boto3.client("athena",
                             aws_access_key_id = AWS_ACCESS_KEY,
                             aws_secret_access_key=AWS_SECRET_KEY,
                             region_name=AWS_REGION,
                             )
```

## Run the athena query👇


```python
def athena_query(SqlQuery:str):

    query_response= athena_client.start_query_execution(
        QueryString= SqlQuery,

        ResultConfiguration={
            "OutputLocation": S3_STAGING_DIR,
            "EncryptionConfiguration":{"EncryptionOption": "SSE_S3"},
        },
    )
    return query_response
    
```


```python
query_response = athena_query('SELECT * FROM "covid-19"."enigma_jhud";')
```

<span style ="font-weight:400;font-size:17px">
<body>
    <p> • After running the query we get response in the dictionery form </p>
  </body>
</span>


```python
query_response
```


    {'QueryExecutionId': '55a5db55-1265-42b1-b0d0-f1982b456571',
     'ResponseMetadata': {'RequestId': '2318ec13-0206-40a2-9e3b-d2da901a6c9e',
      'HTTPStatusCode': 200,
      'HTTPHeaders': {'date': 'Mon, 08 May 2023 11:52:43 GMT',
       'content-type': 'application/x-amz-json-1.1',
       'content-length': '59',
       'connection': 'keep-alive',
       'x-amzn-requestid': '2318ec13-0206-40a2-9e3b-d2da901a6c9e'},
      'RetryAttempts': 0}}


<span style ="font-weight:400;font-size:17px">
<body>
    <p>
    • The QueryExecutionId is used to locate file in S3
    </p>
  </body>
</span>


```python
query_response['QueryExecutionId']
```


    '55a5db55-1265-42b1-b0d0-f1982b456571'


<span style ="font-weight:400;font-size:17px">
<body>
    <p> • Sample of how file query response file location and file name looks like in S3 bucket </p>
  </body>
</span>



```python
f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv"
```


    'output/55a5db55-1265-42b1-b0d0-f1982b456571.csv'


---

## Below 👇 code gets the athena query response (dataset) from the S3 bucket and stores it into the local file


```python
def download_and_load_query_results(query_response:dict,file_name:str):

    temp_file_location: str =f"D:\AWS\de_project\{file_name}.csv"
    s3_client =boto3.client(
        "s3",
        aws_access_key_id = AWS_ACCESS_KEY,
        aws_secret_access_key = AWS_SECRET_KEY,
        region_name = AWS_REGION,
    )
    s3_client.download_file(
        S3_BUCKET_NAME,
        f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv",
        temp_file_location,
    )
    return pd.read_csv(temp_file_location)
```

## Getting datasets from S3 bucket to the local file

### Dataset: Enigma_jhud


```python
enigma_jhud = download_and_load_query_results(query_response,"enigma_jhud")
```


```python
enigma_jhud.head()
```



<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>fips</th>
      <th>admin2</th>
      <th>province_state</th>
      <th>country_region</th>
      <th>last_update</th>
      <th>latitude</th>
      <th>longitude</th>
      <th>confirmed</th>
      <th>deaths</th>
      <th>recovered</th>
      <th>active</th>
      <th>combined_key</th>
      <th>partition_0</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>Anhui</td>
      <td>China</td>
      <td>2020-01-22T17:00:00</td>
      <td>31.826</td>
      <td>117.226</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>"Anhui</td>
      <td>csv</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>Beijing</td>
      <td>China</td>
      <td>2020-01-22T17:00:00</td>
      <td>40.182</td>
      <td>116.414</td>
      <td>14.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>"Beijing</td>
      <td>csv</td>
    </tr>
    <tr>
      <th>2</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>Chongqing</td>
      <td>China</td>
      <td>2020-01-22T17:00:00</td>
      <td>30.057</td>
      <td>107.874</td>
      <td>6.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>"Chongqing</td>
      <td>csv</td>
    </tr>
    <tr>
      <th>3</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>Fujian</td>
      <td>China</td>
      <td>2020-01-22T17:00:00</td>
      <td>26.079</td>
      <td>117.987</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>"Fujian</td>
      <td>csv</td>
    </tr>
    <tr>
      <th>4</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>Gansu</td>
      <td>China</td>
      <td>2020-01-22T17:00:00</td>
      <td>36.061</td>
      <td>103.834</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>"Gansu</td>
      <td>csv</td>
    </tr>
  </tbody>
</table>
</div>


### Dataset:  nytimes-data-in-usa-country


```python
query_response = athena_query('SELECT * FROM "covid-19"."nytimes-data-in-usa-countryus_county";')
```


```python
nytimes_data_in_usa_country = download_and_load_query_results(query_response,"nytimes_data_in_usa_country")
```


```python
nytimes_data_in_usa_country.head()
```





<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>county</th>
      <th>state</th>
      <th>fips</th>
      <th>cases</th>
      <th>deaths</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2020-01-21</td>
      <td>Snohomish</td>
      <td>Washington</td>
      <td>53061.0</td>
      <td>1.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2020-01-22</td>
      <td>Snohomish</td>
      <td>Washington</td>
      <td>53061.0</td>
      <td>1.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2020-01-23</td>
      <td>Snohomish</td>
      <td>Washington</td>
      <td>53061.0</td>
      <td>1.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2020-01-24</td>
      <td>Cook</td>
      <td>Illinois</td>
      <td>17031.0</td>
      <td>1.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2020-01-24</td>
      <td>Snohomish</td>
      <td>Washington</td>
      <td>53061.0</td>
      <td>1.0</td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>
</div>



### Dataset: nytimes_data_in_usa_state


```python
query_response = athena_query('SELECT * FROM "covid-19"."nytimes-data-in-usa-statesus_states";')
```


```python
nytimes_data_in_usa_statesus_states = download_and_load_query_results(query_response,"nytimes_data_in_usa_states")
```


```python
nytimes_data_in_usa_statesus_states.head()
```





<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>state</th>
      <th>fips</th>
      <th>cases</th>
      <th>deaths</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2020-01-21</td>
      <td>Washington</td>
      <td>53</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2020-01-22</td>
      <td>Washington</td>
      <td>53</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2020-01-23</td>
      <td>Washington</td>
      <td>53</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2020-01-24</td>
      <td>Illinois</td>
      <td>17</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2020-01-24</td>
      <td>Washington</td>
      <td>53</td>
      <td>1</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>



### Dataset: rearc_covid_19_testing_data_states_daily


```python
query_response = athena_query('SELECT * FROM "covid-19"."rearc-covid-19-testing-data-states-dailystates_daily";')

```


```python
rearc_covid_19_testing_data_states_daily = download_and_load_query_results(query_response,"rearc_covid_19_testing_data_states_daily")
```


```python
rearc_covid_19_testing_data_states_daily.head()
```





<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>state</th>
      <th>positive</th>
      <th>probablecases</th>
      <th>negative</th>
      <th>pending</th>
      <th>totaltestresultssource</th>
      <th>totaltestresults</th>
      <th>hospitalizedcurrently</th>
      <th>hospitalizedcumulative</th>
      <th>...</th>
      <th>dataqualitygrade</th>
      <th>deathincrease</th>
      <th>hospitalizedincrease</th>
      <th>hash</th>
      <th>commercialscore</th>
      <th>negativeregularscore</th>
      <th>negativescore</th>
      <th>positivescore</th>
      <th>score</th>
      <th>grade</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>20210307</td>
      <td>AK</td>
      <td>56886</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>totalTestsViral</td>
      <td>1731628</td>
      <td>33.0</td>
      <td>1293.0</td>
      <td>...</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>dc4bccd4bb885349d7e94d6fed058e285d4be164</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>20210307</td>
      <td>AL</td>
      <td>499819</td>
      <td>107742.0</td>
      <td>1931711.0</td>
      <td>NaN</td>
      <td>totalTestsPeopleViral</td>
      <td>2323788</td>
      <td>494.0</td>
      <td>45976.0</td>
      <td>...</td>
      <td>NaN</td>
      <td>-1.0</td>
      <td>0.0</td>
      <td>997207b430824ea40b8eb8506c19a93e07bc972e</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>20210307</td>
      <td>AR</td>
      <td>324818</td>
      <td>69092.0</td>
      <td>2480716.0</td>
      <td>NaN</td>
      <td>totalTestsViral</td>
      <td>2736442</td>
      <td>335.0</td>
      <td>14926.0</td>
      <td>...</td>
      <td>NaN</td>
      <td>22.0</td>
      <td>11.0</td>
      <td>50921aeefba3e30d31623aa495b47fb2ecc72fae</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>20210307</td>
      <td>AS</td>
      <td>0</td>
      <td>NaN</td>
      <td>2140.0</td>
      <td>NaN</td>
      <td>totalTestsViral</td>
      <td>2140</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>96d23f888c995b9a7f3b4b864de6414f45c728ff</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>20210307</td>
      <td>AZ</td>
      <td>826454</td>
      <td>56519.0</td>
      <td>3073010.0</td>
      <td>NaN</td>
      <td>totalTestsViral</td>
      <td>7908105</td>
      <td>963.0</td>
      <td>57907.0</td>
      <td>...</td>
      <td>NaN</td>
      <td>5.0</td>
      <td>44.0</td>
      <td>0437a7a96f4471666f775e63e86923eb5cbd8cdf</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 56 columns</p>
</div>



### Dataset: rearc-covid-19-testing-data-us-daily


```python
query_response = athena_query('SELECT * FROM "covid-19"."rearc-covid-19-testing-data-us-dailyus_daily";')
```


```python
rearc_covid_19_testing_data_us_daily = download_and_load_query_results(query_response,"rearc_covid_19_testing_data_us_daily")
```


```python
rearc_covid_19_testing_data_us_daily.head()
```





<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>states</th>
      <th>positive</th>
      <th>negative</th>
      <th>pending</th>
      <th>hospitalizedcurrently</th>
      <th>hospitalizedcumulative</th>
      <th>inicucurrently</th>
      <th>inicucumulative</th>
      <th>onventilatorcurrently</th>
      <th>...</th>
      <th>lastmodified</th>
      <th>recovered</th>
      <th>total</th>
      <th>posneg</th>
      <th>deathincrease</th>
      <th>hospitalizedincrease</th>
      <th>negativeincrease</th>
      <th>positiveincrease</th>
      <th>totaltestresultsincrease</th>
      <th>hash</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>20210307</td>
      <td>56</td>
      <td>28755524.0</td>
      <td>74579770.0</td>
      <td>11808.0</td>
      <td>40212.0</td>
      <td>878613.0</td>
      <td>8137.0</td>
      <td>45475.0</td>
      <td>2801.0</td>
      <td>...</td>
      <td>2021-03-07T24:00:00Z</td>
      <td>NaN</td>
      <td>0</td>
      <td>0</td>
      <td>839</td>
      <td>726</td>
      <td>130414</td>
      <td>41265</td>
      <td>1156241</td>
      <td>8b26839690cd05c0cef69cb9ed85641a76b5e78e</td>
    </tr>
    <tr>
      <th>1</th>
      <td>20210306</td>
      <td>56</td>
      <td>28714259.0</td>
      <td>74449356.0</td>
      <td>11783.0</td>
      <td>41401.0</td>
      <td>877887.0</td>
      <td>8409.0</td>
      <td>45453.0</td>
      <td>2811.0</td>
      <td>...</td>
      <td>2021-03-06T24:00:00Z</td>
      <td>NaN</td>
      <td>0</td>
      <td>0</td>
      <td>1674</td>
      <td>503</td>
      <td>142201</td>
      <td>59620</td>
      <td>1409138</td>
      <td>d0c0482ea549c9d5c04a7c86acb6fc6a8095a592</td>
    </tr>
    <tr>
      <th>2</th>
      <td>20210305</td>
      <td>56</td>
      <td>28654639.0</td>
      <td>74307155.0</td>
      <td>12213.0</td>
      <td>42541.0</td>
      <td>877384.0</td>
      <td>8634.0</td>
      <td>45373.0</td>
      <td>2889.0</td>
      <td>...</td>
      <td>2021-03-05T24:00:00Z</td>
      <td>NaN</td>
      <td>0</td>
      <td>0</td>
      <td>2221</td>
      <td>2781</td>
      <td>271917</td>
      <td>68787</td>
      <td>1744417</td>
      <td>a35ea4289cec4bb55c9f29ae04ec0fd5ac4e0222</td>
    </tr>
    <tr>
      <th>3</th>
      <td>20210304</td>
      <td>56</td>
      <td>28585852.0</td>
      <td>74035238.0</td>
      <td>12405.0</td>
      <td>44172.0</td>
      <td>874603.0</td>
      <td>8970.0</td>
      <td>45293.0</td>
      <td>2973.0</td>
      <td>...</td>
      <td>2021-03-04T24:00:00Z</td>
      <td>NaN</td>
      <td>0</td>
      <td>0</td>
      <td>1743</td>
      <td>1530</td>
      <td>177957</td>
      <td>65487</td>
      <td>1590984</td>
      <td>a19ad6379a653834cbda3093791ad2c3b9fab5ff</td>
    </tr>
    <tr>
      <th>4</th>
      <td>20210303</td>
      <td>56</td>
      <td>28520365.0</td>
      <td>73857281.0</td>
      <td>11778.0</td>
      <td>45462.0</td>
      <td>873073.0</td>
      <td>9359.0</td>
      <td>45214.0</td>
      <td>3094.0</td>
      <td>...</td>
      <td>2021-03-03T24:00:00Z</td>
      <td>NaN</td>
      <td>0</td>
      <td>0</td>
      <td>2449</td>
      <td>2172</td>
      <td>267001</td>
      <td>66836</td>
      <td>1406795</td>
      <td>9e1d2afda1b0ec243060d6f68a7134d011c0cb2a</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 25 columns</p>
</div>



### Dataset: rearc-covid-19-testing-data-us-total-latest


```python
query_response = athena_query('SELECT * FROM "covid-19"."rearc-covid-19-testing-data-us-total-latestus_total_latest";')
```


```python
rearc_covid_19_testing_data_us_totallatest = download_and_load_query_results(query_response,"rearc_covid_19_testing_data_us_totallatest")
```


```python
rearc_covid_19_testing_data_us_totallatest.head()
```





<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>positive</th>
      <th>negative</th>
      <th>pending</th>
      <th>hospitalizedcurrently</th>
      <th>hospitalizedcumulative</th>
      <th>inicucurrently</th>
      <th>inicucumulative</th>
      <th>onventilatorcurrently</th>
      <th>onventilatorcumulative</th>
      <th>recovered</th>
      <th>hash</th>
      <th>lastmodified</th>
      <th>death</th>
      <th>hospitalized</th>
      <th>total</th>
      <th>totaltestresults</th>
      <th>posneg</th>
      <th>notes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1061101</td>
      <td>5170081</td>
      <td>2775</td>
      <td>53793</td>
      <td>111955</td>
      <td>9486</td>
      <td>4192</td>
      <td>4712</td>
      <td>373</td>
      <td>153947</td>
      <td>95064ba29ccbc20dbec397033dfe4b1f45137c99</td>
      <td>2020-05-01T09:12:31.891Z</td>
      <td>57266</td>
      <td>111955</td>
      <td>6233957</td>
      <td>6231182</td>
      <td>6231182</td>
      <td>"NOTE: ""total""</td>
    </tr>
  </tbody>
</table>
</div>



### Dataset: rearc_usa_hospital_beds


```python
query_response = athena_query('SELECT * FROM "covid-19"."rearc_usa_hospital_bedsrearc_usa_hospital_beds";')
```


```python
rearc_usa_hospital_beds = download_and_load_query_results(query_response,"rearc_usa_hospital_beds")
```


```python
rearc_usa_hospital_beds.head()
```





<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>objectid</th>
      <th>hospital_name</th>
      <th>hospital_type</th>
      <th>hq_address</th>
      <th>hq_address1</th>
      <th>hq_city</th>
      <th>hq_state</th>
      <th>hq_zip_code</th>
      <th>county_name</th>
      <th>state_name</th>
      <th>...</th>
      <th>num_licensed_beds</th>
      <th>num_staffed_beds</th>
      <th>num_icu_beds</th>
      <th>adult_icu_beds</th>
      <th>pedi_icu_beds</th>
      <th>bed_utilization</th>
      <th>avg_ventilator_usage</th>
      <th>potential_increase_in_bed_capac</th>
      <th>latitude</th>
      <th>longtitude</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Phoenix VA Health Care System (AKA Carl T Hayd...</td>
      <td>VA Hospital</td>
      <td>650 E Indian School Rd</td>
      <td>NaN</td>
      <td>Phoenix</td>
      <td>AZ</td>
      <td>85012</td>
      <td>Maricopa</td>
      <td>Arizona</td>
      <td>...</td>
      <td>129.0</td>
      <td>129.0</td>
      <td>0</td>
      <td>0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>0</td>
      <td>33.495498</td>
      <td>-112.066157</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Southern Arizona VA Health Care System</td>
      <td>VA Hospital</td>
      <td>3601 S 6th Ave</td>
      <td>NaN</td>
      <td>Tucson</td>
      <td>AZ</td>
      <td>85723</td>
      <td>Pima</td>
      <td>Arizona</td>
      <td>...</td>
      <td>295.0</td>
      <td>295.0</td>
      <td>2</td>
      <td>2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2.0</td>
      <td>0</td>
      <td>32.181263</td>
      <td>-110.965885</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>VA Central California Health Care System</td>
      <td>VA Hospital</td>
      <td>2615 E Clinton Ave</td>
      <td>NaN</td>
      <td>Fresno</td>
      <td>CA</td>
      <td>93703</td>
      <td>Fresno</td>
      <td>California</td>
      <td>...</td>
      <td>57.0</td>
      <td>57.0</td>
      <td>2</td>
      <td>2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2.0</td>
      <td>0</td>
      <td>36.773324</td>
      <td>-119.779742</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>VA Connecticut Healthcare System - West Haven ...</td>
      <td>VA Hospital</td>
      <td>950 Campbell Ave</td>
      <td>NaN</td>
      <td>West Haven</td>
      <td>CT</td>
      <td>6516</td>
      <td>New Haven</td>
      <td>Connecticut</td>
      <td>...</td>
      <td>216.0</td>
      <td>216.0</td>
      <td>1</td>
      <td>1</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2.0</td>
      <td>0</td>
      <td>41.284400</td>
      <td>-72.957610</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Wilmington VA Medical Center</td>
      <td>VA Hospital</td>
      <td>1601 Kirkwood Hwy</td>
      <td>NaN</td>
      <td>Wilmington</td>
      <td>DE</td>
      <td>19805</td>
      <td>New Castle</td>
      <td>Delaware</td>
      <td>...</td>
      <td>60.0</td>
      <td>60.0</td>
      <td>0</td>
      <td>0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>0</td>
      <td>39.740206</td>
      <td>-75.606532</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 23 columns</p>
</div>



### Dataset: static-datasets-countrycode


```python
query_response = athena_query('SELECT * FROM "covid-19"."static-datasets-countrycodecountrycode";')
```


```python
static_datasets_countrycode = download_and_load_query_results(query_response,"static_datasets_countrycode")
```


```python
static_datasets_countrycode.head()
```





<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>country</th>
      <th>alpha-2 code</th>
      <th>alpha-3 code</th>
      <th>numeric code</th>
      <th>latitude</th>
      <th>longitude</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Afghanistan</td>
      <td>AF</td>
      <td>AFG</td>
      <td>4.0</td>
      <td>33.0000</td>
      <td>65.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Albania</td>
      <td>AL</td>
      <td>ALB</td>
      <td>8.0</td>
      <td>41.0000</td>
      <td>20.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Algeria</td>
      <td>DZ</td>
      <td>DZA</td>
      <td>12.0</td>
      <td>28.0000</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>American Samoa</td>
      <td>AS</td>
      <td>ASM</td>
      <td>16.0</td>
      <td>-14.3333</td>
      <td>-170.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Andorra</td>
      <td>AD</td>
      <td>AND</td>
      <td>20.0</td>
      <td>42.5000</td>
      <td>1.6</td>
    </tr>
  </tbody>
</table>
</div>



### Dataset: static_datasets_countypopulation


```python
query_response = athena_query('SELECT * FROM "covid-19"."static_datasets_countypopulationcountypopulation";')
```


```python
static_datasets_countypopulation = download_and_load_query_results(query_response,"static_datasets_countypopulation")
```


```python
static_datasets_countypopulation.head()
```






<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>id2</th>
      <th>county</th>
      <th>state</th>
      <th>population estimate 2018</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0500000US01001</td>
      <td>1001</td>
      <td>Autauga</td>
      <td>Alabama</td>
      <td>55601</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0500000US01003</td>
      <td>1003</td>
      <td>Baldwin</td>
      <td>Alabama</td>
      <td>218022</td>
    </tr>
    <tr>
      <th>2</th>
      <td>0500000US01005</td>
      <td>1005</td>
      <td>Barbour</td>
      <td>Alabama</td>
      <td>24881</td>
    </tr>
    <tr>
      <th>3</th>
      <td>0500000US01007</td>
      <td>1007</td>
      <td>Bibb</td>
      <td>Alabama</td>
      <td>22400</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0500000US01009</td>
      <td>1009</td>
      <td>Blount</td>
      <td>Alabama</td>
      <td>57840</td>
    </tr>
  </tbody>
</table>
</div>



### Dataset: static-datasets-state-abvstate_abv


```python
query_response = athena_query('SELECT * FROM "covid-19"."static-datasets-state-abvstate_abv";')
```


```python
state_abvstate_abv = download_and_load_query_results(query_response,"state_abvstate_abv")
```


```python
state_abvstate_abv.head()
```





<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>State</th>
      <th>Abbreviation</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1</th>
      <td>Alabama</td>
      <td>AL</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Alaska</td>
      <td>AK</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Arizona</td>
      <td>AZ</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Arkansas</td>
      <td>AR</td>
    </tr>
    <tr>
      <th>5</th>
      <td>California</td>
      <td>CA</td>
    </tr>
  </tbody>
</table>
</div>




```python
new_header = state_abvstate_abv.iloc[0]
state_abvstate_abv = state_abvstate_abv[1:]
state_abvstate_abv.columns = new_header
state_abvstate_abv.head()
state_abvstate_abv.to_csv("state_abvstate_abv",index=False)
```





<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>State</th>
      <th>Abbreviation</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1</th>
      <td>Alabama</td>
      <td>AL</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Alaska</td>
      <td>AK</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Arizona</td>
      <td>AZ</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Arkansas</td>
      <td>AR</td>
    </tr>
    <tr>
      <th>5</th>
      <td>California</td>
      <td>CA</td>
    </tr>
  </tbody>
</table>
</div>



---

# Create Data model from the datasets


```python

```

---

# Create Dimensional model from the datasets


```python

```

---

# Create datasets according to the dimensional model


```python
factcovid_1 = enigma_jhud[['fips','province_state','country_region',
                           'confirmed', 'deaths', 'recovered', 'active',
                           ]]
factcovid_2 = rearc_covid_19_testing_data_states_daily[['fips','date','positive','negative','hospitalizedcurrently','hospitalized','hospitalizeddischarged',]]
factCovid = pd.merge(factcovid_1,factcovid_2,on='fips',how='inner')
```


```python
factCovid.head()
```





<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>fips</th>
      <th>province_state</th>
      <th>country_region</th>
      <th>confirmed</th>
      <th>deaths</th>
      <th>recovered</th>
      <th>active</th>
      <th>date</th>
      <th>positive</th>
      <th>negative</th>
      <th>hospitalizedcurrently</th>
      <th>hospitalized</th>
      <th>hospitalizeddischarged</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>NaN</td>
      <td>Anhui</td>
      <td>China</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>20210119</td>
      <td>289939</td>
      <td>NaN</td>
      <td>1066.0</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NaN</td>
      <td>Beijing</td>
      <td>China</td>
      <td>14.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>20210119</td>
      <td>289939</td>
      <td>NaN</td>
      <td>1066.0</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>NaN</td>
      <td>Chongqing</td>
      <td>China</td>
      <td>6.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>20210119</td>
      <td>289939</td>
      <td>NaN</td>
      <td>1066.0</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>NaN</td>
      <td>Fujian</td>
      <td>China</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>20210119</td>
      <td>289939</td>
      <td>NaN</td>
      <td>1066.0</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>NaN</td>
      <td>Gansu</td>
      <td>China</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>20210119</td>
      <td>289939</td>
      <td>NaN</td>
      <td>1066.0</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>




```python
factCovid.shape
```




    (27992, 13)



### • Schema for table factCovid


```python
factCovidsql = pd.io.sql.get_schema(factCovid.reset_index(),'factCovid')
print(''.join(factCovidsql))
```

    CREATE TABLE "factCovid" (
    "index" INTEGER,
      "fips" REAL,
      "province_state" TEXT,
      "country_region" TEXT,
      "confirmed" REAL,
      "deaths" REAL,
      "recovered" REAL,
      "active" REAL,
      "date" INTEGER,
      "positive" INTEGER,
      "negative" REAL,
      "hospitalizedcurrently" REAL,
      "hospitalized" REAL,
      "hospitalizeddischarged" REAL
    )
    


```python
dimRegion_1 = enigma_jhud[['fips','province_state','country_region','latitude', 'longitude',]]
dimRegion_2 = nytimes_data_in_usa_country[['fips','county','state']]
dimRegion = pd.merge(dimRegion_1,dimRegion_2,on='fips',how='inner')
```


```python
dimRegion.head()
```




<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>fips</th>
      <th>province_state</th>
      <th>country_region</th>
      <th>latitude</th>
      <th>longitude</th>
      <th>county</th>
      <th>state</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>NaN</td>
      <td>Anhui</td>
      <td>China</td>
      <td>31.826</td>
      <td>117.226</td>
      <td>New York City</td>
      <td>New York</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NaN</td>
      <td>Anhui</td>
      <td>China</td>
      <td>31.826</td>
      <td>117.226</td>
      <td>Unknown</td>
      <td>Rhode Island</td>
    </tr>
    <tr>
      <th>2</th>
      <td>NaN</td>
      <td>Anhui</td>
      <td>China</td>
      <td>31.826</td>
      <td>117.226</td>
      <td>New York City</td>
      <td>New York</td>
    </tr>
    <tr>
      <th>3</th>
      <td>NaN</td>
      <td>Anhui</td>
      <td>China</td>
      <td>31.826</td>
      <td>117.226</td>
      <td>Unknown</td>
      <td>Rhode Island</td>
    </tr>
    <tr>
      <th>4</th>
      <td>NaN</td>
      <td>Anhui</td>
      <td>China</td>
      <td>31.826</td>
      <td>117.226</td>
      <td>New York City</td>
      <td>New York</td>
    </tr>
  </tbody>
</table>
</div>




```python
dimRegion.shape
```




    (11752274, 7)



### • Schema for table dimRegion


```python
dimRegionsql = pd.io.sql.get_schema(dimRegion.reset_index(),'dimRegion')
print(''.join(dimRegionsql))
```

    CREATE TABLE "dimRegion" (
    "index" INTEGER,
      "fips" REAL,
      "province_state" TEXT,
      "country_region" TEXT,
      "latitude" REAL,
      "longitude" REAL,
      "county" TEXT,
      "state" TEXT
    )
    


```python
dimHospital=rearc_usa_hospital_beds[['fips','state_name','latitude','longtitude','hq_address','hospital_name','hospital_type','hq_city','hq_state']]
```


```python
dimHospital.head()
```





<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>fips</th>
      <th>state_name</th>
      <th>latitude</th>
      <th>longtitude</th>
      <th>hq_address</th>
      <th>hospital_name</th>
      <th>hospital_type</th>
      <th>hq_city</th>
      <th>hq_state</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>4013.0</td>
      <td>Arizona</td>
      <td>33.495498</td>
      <td>-112.066157</td>
      <td>650 E Indian School Rd</td>
      <td>Phoenix VA Health Care System (AKA Carl T Hayd...</td>
      <td>VA Hospital</td>
      <td>Phoenix</td>
      <td>AZ</td>
    </tr>
    <tr>
      <th>1</th>
      <td>4019.0</td>
      <td>Arizona</td>
      <td>32.181263</td>
      <td>-110.965885</td>
      <td>3601 S 6th Ave</td>
      <td>Southern Arizona VA Health Care System</td>
      <td>VA Hospital</td>
      <td>Tucson</td>
      <td>AZ</td>
    </tr>
    <tr>
      <th>2</th>
      <td>6019.0</td>
      <td>California</td>
      <td>36.773324</td>
      <td>-119.779742</td>
      <td>2615 E Clinton Ave</td>
      <td>VA Central California Health Care System</td>
      <td>VA Hospital</td>
      <td>Fresno</td>
      <td>CA</td>
    </tr>
    <tr>
      <th>3</th>
      <td>9009.0</td>
      <td>Connecticut</td>
      <td>41.284400</td>
      <td>-72.957610</td>
      <td>950 Campbell Ave</td>
      <td>VA Connecticut Healthcare System - West Haven ...</td>
      <td>VA Hospital</td>
      <td>West Haven</td>
      <td>CT</td>
    </tr>
    <tr>
      <th>4</th>
      <td>10003.0</td>
      <td>Delaware</td>
      <td>39.740206</td>
      <td>-75.606532</td>
      <td>1601 Kirkwood Hwy</td>
      <td>Wilmington VA Medical Center</td>
      <td>VA Hospital</td>
      <td>Wilmington</td>
      <td>DE</td>
    </tr>
  </tbody>
</table>
</div>




```python
dimHospital.shape
```




    (6637, 9)



### • Schema for table dimHospital


```python
dimHospitalsql = pd.io.sql.get_schema(dimHospital.reset_index(),'dimHospital')
print(''.join(dimHospitalsql))
```

    CREATE TABLE "dimHospital" (
    "index" INTEGER,
      "fips" REAL,
      "state_name" TEXT,
      "latitude" REAL,
      "longtitude" REAL,
      "hq_address" TEXT,
      "hospital_name" TEXT,
      "hospital_type" TEXT,
      "hq_city" TEXT,
      "hq_state" TEXT
    )
    


```python
dimDate = rearc_covid_19_testing_data_states_daily[['fips','date']]
```


```python
dimDate.head()
```





<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>fips</th>
      <th>date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2.0</td>
      <td>20210307</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1.0</td>
      <td>20210307</td>
    </tr>
    <tr>
      <th>2</th>
      <td>5.0</td>
      <td>20210307</td>
    </tr>
    <tr>
      <th>3</th>
      <td>60.0</td>
      <td>20210307</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4.0</td>
      <td>20210307</td>
    </tr>
  </tbody>
</table>
</div>






```python
dimDate.shape
```




    (2685, 2)




```python
dimDate['date']=pd.to_datetime(dimDate['date'],format='%Y%m%d')
```

    C:\Users\Durgesh patil\AppData\Local\Temp\ipykernel_19020\4062609722.py:1: SettingWithCopyWarning: 
    A value is trying to be set on a copy of a slice from a DataFrame.
    Try using .loc[row_indexer,col_indexer] = value instead
    
    See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
      dimDate['date']=pd.to_datetime(dimDate['date'],format='%Y%m%d')
    


```python
dimDate.head()
```





<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>fips</th>
      <th>date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2.0</td>
      <td>2021-03-07</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1.0</td>
      <td>2021-03-07</td>
    </tr>
    <tr>
      <th>2</th>
      <td>5.0</td>
      <td>2021-03-07</td>
    </tr>
    <tr>
      <th>3</th>
      <td>60.0</td>
      <td>2021-03-07</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4.0</td>
      <td>2021-03-07</td>
    </tr>
  </tbody>
</table>
</div>




```python
dimDate['year'] = dimDate['date'].dt.year
dimDate['month'] = dimDate['date'].dt.month
dimDate['day_of_week'] = dimDate['date'].dt.dayofweek
```

    C:\Users\Durgesh patil\AppData\Local\Temp\ipykernel_19020\935310350.py:1: SettingWithCopyWarning: 
    A value is trying to be set on a copy of a slice from a DataFrame.
    Try using .loc[row_indexer,col_indexer] = value instead
    
    See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
      dimDate['year'] = dimDate['date'].dt.year
    C:\Users\Durgesh patil\AppData\Local\Temp\ipykernel_19020\935310350.py:2: SettingWithCopyWarning: 
    A value is trying to be set on a copy of a slice from a DataFrame.
    Try using .loc[row_indexer,col_indexer] = value instead
    
    See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
      dimDate['month'] = dimDate['date'].dt.month
    C:\Users\Durgesh patil\AppData\Local\Temp\ipykernel_19020\935310350.py:3: SettingWithCopyWarning: 
    A value is trying to be set on a copy of a slice from a DataFrame.
    Try using .loc[row_indexer,col_indexer] = value instead
    
    See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
      dimDate['day_of_week'] = dimDate['date'].dt.dayofweek
    


```python
dimDate.head()
```





<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>fips</th>
      <th>date</th>
      <th>year</th>
      <th>month</th>
      <th>day_of_week</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2.0</td>
      <td>2021-03-07</td>
      <td>2021</td>
      <td>3</td>
      <td>6</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1.0</td>
      <td>2021-03-07</td>
      <td>2021</td>
      <td>3</td>
      <td>6</td>
    </tr>
    <tr>
      <th>2</th>
      <td>5.0</td>
      <td>2021-03-07</td>
      <td>2021</td>
      <td>3</td>
      <td>6</td>
    </tr>
    <tr>
      <th>3</th>
      <td>60.0</td>
      <td>2021-03-07</td>
      <td>2021</td>
      <td>3</td>
      <td>6</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4.0</td>
      <td>2021-03-07</td>
      <td>2021</td>
      <td>3</td>
      <td>6</td>
    </tr>
  </tbody>
</table>
</div>



### • Schema for the dimDate table


```python
dimDatesql = pd.io.sql.get_schema(dimDate.reset_index(),'dimDate')
print(''.join(dimDatesql))
```

    CREATE TABLE "dimDate" (
    "index" INTEGER,
      "fips" REAL,
      "date" TIMESTAMP,
      "year" INTEGER,
      "month" INTEGER,
      "day_of_week" INTEGER
    )
    


```python
bucket = 'durgesh-covid-project'#Already created in S3
```


```python
s3_client =boto3.client(
        "s3",
        aws_access_key_id = AWS_ACCESS_KEY,
        aws_secret_access_key = AWS_SECRET_KEY,
        region_name = AWS_REGION,
    )
```


```python
csv_buffer = StringIO()
factCovid.to_csv(csv_buffer)
s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key='output/factCovid.csv')
```




    {'ResponseMetadata': {'RequestId': '5PBDVZ2GV9DMAF4Q',
      'HostId': 'b8WWNJ67ig9IyfMSXfz/QGovMeDPxBNIIo87PyWdB1MyXkNRb6XLJQY0URjVC4D5eOPckbxHE4DV1bCwdMJ2rQ==',
      'HTTPStatusCode': 200,
      'HTTPHeaders': {'x-amz-id-2': 'b8WWNJ67ig9IyfMSXfz/QGovMeDPxBNIIo87PyWdB1MyXkNRb6XLJQY0URjVC4D5eOPckbxHE4DV1bCwdMJ2rQ==',
       'x-amz-request-id': '5PBDVZ2GV9DMAF4Q',
       'date': 'Mon, 08 May 2023 18:46:17 GMT',
       'x-amz-version-id': '17oSOSl722ZV_RAFHm9LG5JpqoPhj1Cx',
       'x-amz-server-side-encryption': 'AES256',
       'etag': '"fc9be4f5fc50864df1f4ed534cf29988"',
       'server': 'AmazonS3',
       'content-length': '0'},
      'RetryAttempts': 0},
     'ETag': '"fc9be4f5fc50864df1f4ed534cf29988"',
     'ServerSideEncryption': 'AES256',
     'VersionId': '17oSOSl722ZV_RAFHm9LG5JpqoPhj1Cx'}




```python
csv_buffer = StringIO()
dimRegion.to_csv(csv_buffer)
s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key='output/dimRegion.csv')
```




    {'ResponseMetadata': {'RequestId': 'FYR6NHKNZT82067Q',
      'HostId': 'cYQOcBt7GOXDNcywH5MsCUxkGj7/cAGqYEEguY/hUDPTFCoBYvnfJgeKpYua7mhBei24tUHs9Wg=',
      'HTTPStatusCode': 200,
      'HTTPHeaders': {'x-amz-id-2': 'cYQOcBt7GOXDNcywH5MsCUxkGj7/cAGqYEEguY/hUDPTFCoBYvnfJgeKpYua7mhBei24tUHs9Wg=',
       'x-amz-request-id': 'FYR6NHKNZT82067Q',
       'date': 'Mon, 08 May 2023 18:57:32 GMT',
       'x-amz-version-id': 'jEEwpk7n174NSeUN_.gK8P9p0hRSvqhJ',
       'x-amz-server-side-encryption': 'AES256',
       'etag': '"2fdf65265c31f0f6e8c02b4bd066d670"',
       'server': 'AmazonS3',
       'content-length': '0'},
      'RetryAttempts': 0},
     'ETag': '"2fdf65265c31f0f6e8c02b4bd066d670"',
     'ServerSideEncryption': 'AES256',
     'VersionId': 'jEEwpk7n174NSeUN_.gK8P9p0hRSvqhJ'}




```python
csv_buffer = StringIO()
dimHospital.to_csv(csv_buffer)
s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key='output/dimHospital.csv')
```




    {'ResponseMetadata': {'RequestId': 'G19Q0FMH1T83GVM9',
      'HostId': '8gjGOKxCxNcbE54ausule1oBvlSJANlu4HzpetQcbRWcj3RCn32b45dvwjWmS7JCW5lL2CXBDFY=',
      'HTTPStatusCode': 200,
      'HTTPHeaders': {'x-amz-id-2': '8gjGOKxCxNcbE54ausule1oBvlSJANlu4HzpetQcbRWcj3RCn32b45dvwjWmS7JCW5lL2CXBDFY=',
       'x-amz-request-id': 'G19Q0FMH1T83GVM9',
       'date': 'Mon, 08 May 2023 18:56:00 GMT',
       'x-amz-version-id': '4MgQplRT6wYyQ4.OhOR4tyCh_pidkds.',
       'x-amz-server-side-encryption': 'AES256',
       'etag': '"a26c4e35d128fe6f64955ba9aac1d221"',
       'server': 'AmazonS3',
       'content-length': '0'},
      'RetryAttempts': 0},
     'ETag': '"a26c4e35d128fe6f64955ba9aac1d221"',
     'ServerSideEncryption': 'AES256',
     'VersionId': '4MgQplRT6wYyQ4.OhOR4tyCh_pidkds.'}




```python
csv_buffer = StringIO()
dimDate.to_csv(csv_buffer)
s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key='output/dimDate.csv')
```




    {'ResponseMetadata': {'RequestId': 'TFBZJ33FBSZ8C4MZ',
      'HostId': 'Zzr1eWbEk7MxfS8z16zpvckVav83Q+m4oKaeXHP1skLupv2I6i/LhJKh61MVcUKeXsMgMbLgrkM=',
      'HTTPStatusCode': 200,
      'HTTPHeaders': {'x-amz-id-2': 'Zzr1eWbEk7MxfS8z16zpvckVav83Q+m4oKaeXHP1skLupv2I6i/LhJKh61MVcUKeXsMgMbLgrkM=',
       'x-amz-request-id': 'TFBZJ33FBSZ8C4MZ',
       'date': 'Mon, 08 May 2023 18:56:43 GMT',
       'x-amz-version-id': 'rimJk6e4kj3WXDeuQ1OJRYPf8cAVsLzz',
       'x-amz-server-side-encryption': 'AES256',
       'etag': '"19eb0b77e7f7441c686829bc3fd1a906"',
       'server': 'AmazonS3',
       'content-length': '0'},
      'RetryAttempts': 0},
     'ETag': '"19eb0b77e7f7441c686829bc3fd1a906"',
     'ServerSideEncryption': 'AES256',
     'VersionId': 'rimJk6e4kj3WXDeuQ1OJRYPf8cAVsLzz'}



# Configuration of Cluster 👇


```python
DWH_CLUSTER_TYPE = "single-node"
DWH_NUM_NODES = "1"
DWH_NODE_TYPE = "dc2.large"
DWH_CLUSTER_IDENTIFIER="covidproject"
DWH_DB="flight"
DWH_DB_USER = "durgesh"
DWH_DB_PASSWORD = "Password123"
DWH_PORT = "5439"
DWH_IAM_ROLE_NAME = "redshift_s3_access"
DWH_DB_NAME ="covid-db"
```

### • Creating variables storing boto3.client for redshift & Iam 


```python
redshift_client = boto3.client("redshift",
                             aws_access_key_id = AWS_ACCESS_KEY,
                             aws_secret_access_key=AWS_SECRET_KEY,
                             region_name=AWS_REGION,
                             )
```


```python
iam_client = boto3.client("iam",
                             aws_access_key_id = AWS_ACCESS_KEY,
                             aws_secret_access_key=AWS_SECRET_KEY,
                             region_name=AWS_REGION,
                             )
```

### • Creating variable storing boto3.resource for ec2


```python
ec2_resource = boto3.resource("ec2",
                          aws_access_key_id = AWS_ACCESS_KEY,
                          aws_secret_access_key=AWS_SECRET_KEY,
                          region_name=AWS_REGION,
                          )
```


```python
roleArn = iam_client.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
```


```python
roleArn
```




    'arn:aws:iam::436117849909:role/redshift_s3_access'



## Create Redshift Cluster


```python

redshift_response = redshift_client.create_cluster(
    ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
    NodeType = DWH_NODE_TYPE,
    MasterUsername = DWH_DB_USER,
    MasterUserPassword= DWH_DB_PASSWORD,
    ClusterType = DWH_CLUSTER_TYPE,
    DBName = DWH_DB_NAME,
    
    #Role for S3 access
    IamRoles = [roleArn]
)

```

<span style ="font-weight:400;font-size:17px">
<body>
    <p> • Below response shows the configuration of the cluster created </p>
  </body>
</span>



```python
redshift_response
```


    {'Cluster': {'ClusterIdentifier': 'covidproject',
      'NodeType': 'dc2.large',
      'ClusterStatus': 'creating',
      'ClusterAvailabilityStatus': 'Modifying',
      'MasterUsername': 'durgesh',
      'DBName': 'covid-db',
      'AutomatedSnapshotRetentionPeriod': 1,
      'ManualSnapshotRetentionPeriod': -1,
      'ClusterSecurityGroups': [],
      'VpcSecurityGroups': [{'VpcSecurityGroupId': 'sg-0a1c0b3f4a736fcba',
        'Status': 'active'}],
      'ClusterParameterGroups': [{'ParameterGroupName': 'default.redshift-1.0',
        'ParameterApplyStatus': 'in-sync'}],
      'ClusterSubnetGroupName': 'default',
      'VpcId': 'vpc-09eadb7f91dfac446',
      'PreferredMaintenanceWindow': 'sat:07:30-sat:08:00',
      'PendingModifiedValues': {'MasterUserPassword': '****'},
      'ClusterVersion': '1.0',
      'AllowVersionUpgrade': True,
      'NumberOfNodes': 1,
      'PubliclyAccessible': True,
      'Encrypted': False,
      'Tags': [],
      'EnhancedVpcRouting': False,
      'IamRoles': [{'IamRoleArn': 'arn:aws:iam::436117849909:role/redshift_s3_access',
        'ApplyStatus': 'adding'}],
      'MaintenanceTrackName': 'current',
      'DeferredMaintenanceWindows': [],
      'NextMaintenanceWindowStartTime': datetime.datetime(2023, 5, 13, 7, 30, tzinfo=tzutc()),
      'AquaConfiguration': {'AquaStatus': 'disabled',
       'AquaConfigurationStatus': 'auto'}},
     'ResponseMetadata': {'RequestId': '91d285ee-bf62-4629-a120-f991b68b8b7b',
      'HTTPStatusCode': 200,
      'HTTPHeaders': {'x-amzn-requestid': '91d285ee-bf62-4629-a120-f991b68b8b7b',
       'content-type': 'text/xml',
       'content-length': '2463',
       'date': 'Tue, 09 May 2023 07:11:30 GMT'},
      'RetryAttempts': 0}}


<span style ="font-weight:400;font-size:17px">
<body>
    <p><b> • Go to redshift to verify the creation of the cluster</b></p>
  </body>
</span>

## • Changing Security Group Configuration


```python
VpcId = redshift_response['Cluster']['VpcId']
```


```python
vpc = ec2_resource.Vpc(id = VpcId)
defaultSg = list(vpc.security_groups.all())[0]
defaultSg.authorize_ingress(
    GroupName=defaultSg.group_name,
    CidrIp='0.0.0.0/0',
    IpProtocol='TCP',
    FromPort = int(DWH_PORT),
    ToPort = int(DWH_PORT)
)
```




    {'Return': True,
     'SecurityGroupRules': [{'SecurityGroupRuleId': 'sgr-01a04994cce442209',
       'GroupId': 'sg-0a1c0b3f4a736fcba',
       'GroupOwnerId': '436117849909',
       'IsEgress': False,
       'IpProtocol': 'tcp',
       'FromPort': 5439,
       'ToPort': 5439,
       'CidrIpv4': '0.0.0.0/0'}],
     'ResponseMetadata': {'RequestId': '5633f270-1f90-4888-8094-75bcd4fd8e5c',
      'HTTPStatusCode': 200,
      'HTTPHeaders': {'x-amzn-requestid': '5633f270-1f90-4888-8094-75bcd4fd8e5c',
       'cache-control': 'no-cache, no-store',
       'strict-transport-security': 'max-age=31536000; includeSubDomains',
       'content-type': 'text/xml;charset=UTF-8',
       'content-length': '723',
       'date': 'Tue, 09 May 2023 07:47:38 GMT',
       'server': 'AmazonEC2'},
      'RetryAttempts': 0}}



<span style ="font-weight:400;font-size:17px">
<body>
    <p><b> • Go to redshift console and check inside the properties section there you will find Security group option, check whether it is updated.</b></p>
  </body>
</span>

---

# Script for making tables into Redshift 👇

## 1.  Use Redshift connector library to connect to the redshift cluster


```python
import redshift_connector

conn = redshift_connector.connect(
        host='covidproject.cyic4klygty9.ap-south-1.redshift.amazonaws.com',
        database='covid-db',
        user='durgesh',
        password='Password123'
     )

conn.autocommit = True
cursor= redshift_connector.Cursor = conn.cursor()

```

## 2. Create tables into the Redshift datawarehouse


```python

cursor.execute("""
CREATE TABLE "factCovid" (
"index" INTEGER,
"fips" REAL,
"province_state" TEXT,
"country_region" TEXT,
"confirmed" REAL ,
"deaths" REAL,
"recovered" REAL,
"active" REAL,
"date" INTEGER,
"positive" REAL,
"negative" REAL,
"hospitalizedcurrently" REAL,
"hospitalized" REAL,
"hospitalizeddischarged" REAL
)
""")

cursor.execute("""
CREATE TABLE "dimHospital" (
"index" INTEGER,
"fips" REAL,
"state_name" TEXT,
"latitude" REAL,
"longtitude" REAL,
"hq_address" TEXT,
"hospital_type" TEXT,
"hospital_name" TEXT,
"hq_city" TEXT,
"hq_state" TEXT
)
""")

cursor.execute("""
CREATE TABLE "dimRegion" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "latitude" REAL,
  "longitude" REAL,
  "county" TEXT,
  "state" TEXT
)
""")

cursor.execute("""
CREATE TABLE "dimDate" (
"index" INTEGER,
  "fips" REAL,
  "date" TIMESTAMP,
  "year" INTEGER,
  "month" INTEGER,
  "day_of_week" INTEGER
)
""")

```

### 3. Copy data form S3 to the tables


```python
cursor.execute(""" 
copy factCovid from 's3://durgesh-covid-project/output/factCovid.csv'
credentials 'aws_iam_role=arn:aws:iam::436117849909:role/redshift_s3_access'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")

cursor.execute(""" 
copy dimHospital from 's3://durgesh-covid-project/output/dimHospital.csv'
credentials 'aws_iam_role=arn:aws:iam::436117849909:role/redshift_s3_access'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")

cursor.execute(""" 
copy dimRegion from 's3://durgesh-covid-project/output/dimRegion.csv'
credentials 'aws_iam_role=arn:aws:iam::436117849909:role/redshift_s3_access'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")

cursor.execute(""" 
copy dimDate from 's3://durgesh-covid-project/output/dimDate.csv'
credentials 'aws_iam_role=arn:aws:iam::436117849909:role/redshift_s3_access'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")
```

---

# Download the .whl file for redshift-connector from 
### https://pypi.org/project/redshift-connector/2.0.389/

<span style ="font-weight:400;font-size:17px">
<body>
    <p><b> • Upload this file to the packages folder in your S3 bucket, where you are storing query responses .</b></p>
  </body>
</span> 

# Create Glue Jobs to run the Script

## • Create job with Python Shell script editor

![create%20glue%20jobs.png](attachment:create%20glue%20jobs.png)

## • Enter the location/URI of the libraryfile Glue job will use this library to perform code operations.

![Glue%20in_jobdetails&advanced_properties%20add%20path%20of%20s3%20bucket%20object%20or%20URI.png](attachment:Glue%20in_jobdetails&advanced_properties%20add%20path%20of%20s3%20bucket%20object%20or%20URI.png)

## • Save and Run the Script

![Glue%20write%20python%20script%20in%20script%20option.png](attachment:Glue%20write%20python%20script%20in%20script%20option.png)

## After successful Run you will see below 👇 result

![Glue%20run%20successful.png](attachment:Glue%20run%20successful.png)

## Below Image shows the query is successful in creating the tables inside the Redshift cluster database

![script%20added%20tables%20to%20redshift.png](attachment:script%20added%20tables%20to%20redshift.png)

<span style ="font-weight:400;font-size:17px">
<body>
    <p><b> • You can now perform analysis on this data and expand further by connecting it with BI tools to create dashboards.</b></p>
  </body>
</span> 

<html>
<head>
<style>
footer {
  text-align: center;
  padding: 3px;
  background-color: DarkSalmon;
  color: white;
}
</style>
</head>
<body>

<footer>
  <p>Author: Durgesh Patil<br>
  <a href="mailto:amolsp1999@gmail.com">amolsp1999@gmail.com</a></p>
</footer>

</body>
</html>
