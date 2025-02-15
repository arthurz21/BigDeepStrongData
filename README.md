# Final Training Data
[Data Google Drive Link](https://drive.google.com/drive/folders/1041G9YLmHWgw0cwV-kyW7UesygWT4-fJ?usp=sharing)
## Competition Data 
1. `competion_data/comp_trx_level_training_data_dict.pkl`. <br>
    
    - This is the transaction level data which is all cleaned up with the following categories one-hot encoded `debit_credit`, `trx_type`, `cash_indicator`, `country`, `province`, `city`,`merchant_category`, `ecommerce_ind`<br> 
    
    - `transaction_date`  and `transaction_time` has been converted to absolute positional encoding (ie when during the whole transaction period the transaction occured) and cyclic positional_encoding (ie when during the week, day and hour did the transaction occur).
    - Each transaction is now 256 dimensions. 
    - Data is stored as a dictionary pickle. Keys are the customer IDs and the values are all the transactions for the customer. 

2. `competion_data/comp_cust_level_training_data.csv` <br>

    - This the customer level data that we have been working so far

## Synthetic Data
1. `synthetic_data/synth_trx_level_training_data_dict.pkl`. <br>
    
    - Similar to competition data, it is transaction level data but synthetic. Following columns were one-hot encoded - `debit_credit`, `trx_type`, `currency`. There were too many Bank IDs to one hot encode so they were converted to 19-Bit binary encoding.

    - `transaction_date`  and `transaction_time` has been converted to absolute positional encoding (ie when during the whole transaction period the transaction occured) and cyclic positional_encoding (ie when during the week, day and hour did the transaction occur).
    - Each Transaction is 50 dimentions long
    - This dataset also has label column called `Is Laundering`
    - Data is stored as a dictionary pickle. Keys are the customer IDs and the values are all the transactions for the customer.

2. `synthetic_data/synth_cust_level_training_data.csv` <br>

    - This the customer level data that we have been working so far but synthetic