import pandas as pd
import numpy as np
import os
import datetime

class Customer:
    def __init__(self, ID):
        '''
        self.transactions is a dictionary with keys of transaction processes and value of list of lists 
            storing transaction information of all transaction for customer with id: ID
        Each transaction should contain the method of money transfer

        Wire transaction: [amount_cad,debit_credit,transaction_date,transaction_time]
        Cheque transaction: [amount_cad,debit_credit,transaction_date]
        ABM transaction: [amount_cad,debit_credit,cash_indicator,country,province,city,transaction_date,transaction_time]
        Card transaction: [amount_cad,debit_credit,merchant_category,ecommerce_ind,country,province,city,transaction_date,transaction_time]
        EFT (Electronic Funds) transaction: [amount_cad,debit_credit,transaction_date,transaction_time]
        EMT (Email Money) transaction: [amount_cad,debit_credit,transaction_date,transaction_time]
        '''
        self.ID = ID
        self.transactions = dict()
        self.merchant_catagories = []
        # order of all transaction variables is ['abm', 'card', 'cheque', 'eft', 'emt', 'wire']
        self.transaction_count = np.zeros((6,1))
        self.transactions_frequency = np.zeros((6,3))
        # Debit is column 0, Credit is column 2
        self.avg_transaction_amount = np.zeros((6,2))
        self.transaction_amount_std = np.zeros((6,2))

        self.first_date = datetime.datetime(9999, 1, 1)
        self.final_date = datetime.datetime(1, 1, 1)

    def transaction_freq(self):
        '''
        Finds avg transactions per day for each transaction type per
                    day (index 0), week (index 1) and month (index 2)
        Transaction types in order of index: [abm, card, cheque, eft, emt, wire] 
        '''
        num_days = self.final_date - self.first_date
        num_weeks = num_days.days//7
        num_months = num_days.days//30
            
        for i,type in enumerate(['abm', 'card', 'cheque', 'eft', 'emt', 'wire']):
            if type in self.transactions.keys():
                transaction_count = len(self.transactions[type])
                if len(self.transactions[type]) == 0:
                    self.transaction_count[i,0] = transaction_count
                    self.transactions_frequency[i,0] = transaction_count
                    self.transactions_frequency[i,1] = transaction_count
                    self.transactions_frequency[i,2] = transaction_count
                else:
                    if num_days.days == 0:
                        self.transactions_frequency[i,0] = transaction_count
                    else:
                        self.transactions_frequency[i,0] = transaction_count / num_days.days
                    
                    if num_weeks == 0:
                        self.transactions_frequency[i,1] = transaction_count
                    else:
                        self.transactions_frequency[i,1] = transaction_count / num_weeks

                    if num_months == 0:
                        self.transactions_frequency[i,2] = transaction_count
                    else:
                        self.transactions_frequency[i,2] = transaction_count / num_months
                        
                      
                    # self.transaction_count[i,0] = len(self.transactions[type])         
                    # self.transactions_frequency[i,0] = self.transaction_count[i,0] / num_days.days
                    # self.transactions_frequency[i,1] = self.transaction_count[i,0] / num_weeks
                    # self.transactions_frequency[i,2] = self.transaction_count[i,0] / num_months
            

    def transaction_amounts(self):
        '''
        Finds avg transaction amount for each transaction type
        '''
        for i,type in enumerate(['abm', 'card', 'cheque', 'eft', 'emt', 'wire']):
            if type not in self.transactions.keys():
                continue
            total_credited = []
            total_debited = []
            for transaction in self.transactions[type]:
                if transaction[3] in ['credit', 'C']:
                    total_credited.append(transaction[2])
                if transaction[3] in ['debit', 'D']:
                    total_debited.append(transaction[2])
            if len(total_credited) != 0:
                self.avg_transaction_amount[i,1] = sum(total_credited)/len(total_credited)
                self.transaction_amount_std[i,1] = np.std(total_credited)
            else:
                self.avg_transaction_amount[i,1] = 0
                self.transaction_amount_std[i,1] = 0
            
            if len(total_debited) != 0:
                self.avg_transaction_amount[i,0] = sum(total_debited)/len(total_debited)
                self.transaction_amount_std[i,0] = np.std(total_debited)
            else:
                self.avg_transaction_amount[i,0] = 0
                self.transaction_amount_std[i,0] = 0
            

    def add_transaction(self,type,transaction):
        '''
        type should be the transaction process
        transaction is a list of detailed transaction information
        '''
        if type not in ['abm', 'card', 'cheque', 'eft', 'emt', 'wire']: 
            print("*****TRANSACTION TYPE IS NOT FOUND******\n DATA WAS NOT ALLOCATED PLEASE TRY AGAIN")
            return 1
        
        if type not in self.transactions.keys():
            self.transactions[type] = [transaction]
        else:
            self.transactions[type].append(transaction)
        
        if type == 'cheque': 
                date_col = -1 
        else: 
            date_col = -2
        
        date = transaction[date_col].split('-')
        date[0], date[1], date[2] = int(date[0]), int(date[1]), int(date[2])
        if self.first_date > datetime.datetime(date[0],date[1],date[2]):
            self.first_date = datetime.datetime(date[0],date[1],date[2])
        if self.final_date < datetime.datetime(date[0],date[1],date[2]):
            self.final_date = datetime.datetime(date[0],date[1],date[2])

   

    def get_feature_vector(self):
        freq_data = []
        avg_amount = []
        transaction_var = []
        for i,type in enumerate(['abm', 'card', 'cheque', 'eft', 'emt', 'wire']):
            freq_data.append(self.transactions_frequency[i,0])
            freq_data.append(self.transactions_frequency[i,1])
            freq_data.append(self.transactions_frequency[i,2])
            
            avg_amount.append(self.avg_transaction_amount[i,0])
            avg_amount.append(self.avg_transaction_amount[i,1])

            transaction_var.append(self.transaction_amount_std[i,0])
            transaction_var.append(self.transaction_amount_std[i,1])

        feature_vector = [self.ID] + freq_data + avg_amount + transaction_var
        return feature_vector


def get_header():
    freq = ['daily frequency', 'weekly frequency', 'monthly frequency']
    avg = ['debited transaction average', 'credited transaction average']
    var = ['debited transaction std', 'credited transaction std']
    #['abm', 'card', 'cheque', 'eft', 'emt', 'wire']
    freq_header = []
    avg_header = []
    var_header = []
    for i,type in enumerate(['abm', 'card', 'cheque', 'eft', 'emt', 'wire']):
        freq_header.append(type + ' ' + freq[0])
        freq_header.append(type + ' ' + freq[1])
        freq_header.append(type + ' ' + freq[2])

        avg_header.append(type + ' ' + avg[0])
        avg_header.append(type + ' ' + avg[1])

        var_header.append(type + ' ' + var[0])
        var_header.append(type + ' ' + var[1])

    header = ['customer ID'] + freq_header + avg_header + var_header
    return header

if __name__ == "__main__":
    customers = dict()
    # os.chdir("./Raw Data/")
    files = os.listdir()
    for file in files:
        if file not in ['abm.csv', 'card.csv', 'cheque.csv', 'eft.csv', 'emt.csv', 'wire.csv']:
            continue
        data = pd.read_csv(file)
        
        # Because all the credited transactions are negative in the card.csv file 
        # But all the other type of transactions are positive for both credited and debited transactions. 
        if file == 'card.csv': 
            data['amount_cad'] = data['amount_cad'].abs() 
        transaction_type = file.split('.')[0]
        for _, transaction in data.iterrows():
            transaction = transaction.values
            if transaction[1] not in customers.keys():
                cur_customer = Customer(transaction[1])
                cur_customer.add_transaction(transaction_type, transaction) 
                customers[transaction[1]] = cur_customer
            else:
                cur_customer = customers[transaction[1]]
                cur_customer.add_transaction(transaction_type, transaction) 
                customers[transaction[1]] = cur_customer
    
    customer_data = []
    for customer in customers.keys():
        customers[customer].transaction_freq()
        customers[customer].transaction_amounts()        
        customer_data.append(customers[customer].get_feature_vector())

    columns = get_header()

    df = pd.DataFrame(customer_data, columns=columns)

    pd.DataFrame.to_csv(df,'Transaction Volume and Frequency.csv')