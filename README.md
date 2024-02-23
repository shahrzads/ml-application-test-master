# ML Application Engineer Take-Home Exam
This is the solution to ML Application Engineer take-home assignment.

The assignment includes building a low-latency process that will process data, run the endpoints within the `app.py` file in a local application server, send it to ML Endpoints to get predictions, send those prediction results to offers endpoint to get which offers to give to which members, and finally record the results. 

## Setup
In this assignment, [uvicorn](https://www.uvicorn.org/) is used to run a local application to interact with the endpoints defined in `app.py`.

To run the project successfully, you first need to install the environment with the required dependencies. To do that, run the command below in your terminal:
```
pip install -r requirements.txt
```
The requirements.txt file is in the main directory of the project.

## The Task
You will see 3 new files that are responsible for computing, storing, and returning desired values regarding the tasks. The files are as follows:
1. data_processing.py:
   - This file is responsible for processing the data including
       - reading the dataset and handling issues with the dataset (i.e., NaN),
       - transforming the raw features in the dataset to useful features that we need in the future, as below:
            - AVG_POINTS_BOUGHT, calculated as $\text{total points bought} / \text{number of transactions}$
            - AVG_REVENUE_USD, calculated as $\text{total transaction revenue} / \text{number of transactions}$
            - LAST_3_TRANSACTIONS_AVG_POINTS_BOUGHT, which is the AVG_POINTS_BOUGHT for the 3 most recent transactions
            - LAST_3_TRANSACTIONS_AVG_REVENUE_USD, which is the AVG_REVENUE_USD for the 3 most recent transactions
            - PCT_BUY_TRANSACTIONS, calculated as $\text{Number of transactions where transaction type was BUY} / \text{number of transactions}$
            - PCT_GIFT_TRANSACTIONS, calculated as $\text{Number of transactions where transaction type was GIFT} / \text{number of transactions}$
            - PCT_REDEEM_TRANSACTIONS, calculated as $\text{Number of transactions where transaction type was REDEEM} / \text{number of transactions}$
            - DAYS_SINCE_LAST_TRANSACTION, which is the number of days since a member's last transaction. (ie. Current day in UTC - last day of transaction in UTC), and
       - combining these transformed features into a class object (MemberFeatures)
    if you run the command below, it will run all the functions in this file including reading the dataset file, and transforming processes to create new features:
```
python -m src.data_processing
```
Running this command will return and print all the new features.
2. api_interaction.py:
   - This file posts all the data from the previous step as input to predict ATS and RESP endpoints to get the estimated amount and likelihood of purchase respectively.
   - These predictions will then be combined into a class object named Prediction.
   - Then, based on the ATS and RESP predictions, offers will be made to the members.

By running the next command, you will get all the data including member_id, MemberFeatures object (i.e., the transformed features from the previous section), ATS and RESP prediction values, the offer given to the member, and all the latencies for all the function calls.
```
python -m src.api_interaction
```
As you might have understood, this command calls the ```summarize``` function, which calls all the other functions inside it. If you want to see the result from each of the individual functions, all you need to do is call that specific function to run.

3. excel.py
   - This file is responsible for storing all the data that was produced throughout the whole process in an Excel file. Since the goal of  this file is to be used for analyzing the performance as well, the file should include:
   - Member Features (including AVG_POINTS_BOUGHT, AVG_REVENUE_USD, LAST_3_TRANSACTIONS_AVG_POINTS_BOUGHT, LAST_3_TRANSACTIONS_AVG_REVENUE_USD, PCT_BUY_TRANSACTIONS, PCT_GIFT_TRANSACTIONS, PCT_REDEEM_TRANSACTIONS, DAYS_SINCE_LAST_TRANSACTION)
   - Predictions (including ATS and RESP predictions)
   - Offers assigned to each member
   - Latency and throughput of each step (e.g. time taken to read member data, transform each of the features, make predictions, and give offers to the members)
     To store the data in an Excel file, we need a DataFrame, and hence a dictionary. But in this case, we might have nested dictionaries or complex values (i.e., class objects). So, we need functions to flatten the nested dictionaries and extract the class objects as dictionaries.
 The command to save all the required features mentioned above, for a specific member, into an Excel file is as below:
```
python -m src.excel
```    

 After completing all these steps, we need to test and see if each of the functions works correctly. So, for each of the files mentioned above, we have a test file to do the unit test. You can find all the test files in the directory ```./tests```.
 The commands to run each of the unit test files are:
 - For data_processing file:
   ```
   python -m unittest tests.test_data_processing
   ```
 - For api_interaction file:
   ```
   python -m unittest tests.test_api_interaction
   ```
 - For excel file:
   ```
   python -m unittest tests.test_excel
   ```

 Finally, nit tests were integrated into a CI/CD pipeline to run the tests automatically whenever changes are pushed to the version control system. The file can be found in the directory 
 ```
 .github/workflows/main.yml
```

Overall, you can find the following considerations in this project:
- Processing speed/Parallelism/Latency
- Design patterns/programming paradigm
- Scalability
- Documentation
- Clean Code
- Testing
- Automated Testing
- Error Handling
- Readability
- Logging
- CI/CD

