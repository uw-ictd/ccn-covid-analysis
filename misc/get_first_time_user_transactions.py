source_file = "./data/clean/transactions.log"
destination_file = "./data/clean/first_time_user_transactions.csv"

if __name__ == "__main__":
    with open(source_file, "r") as sf:
        with open(destination_file, "w") as df:
            users = set()
            df.write("start,action,user,amount,price,\n")
            for line in sf.readlines():
                tokens = line.strip().split(" ")
                if tokens[2] == "PURCHASE" and tokens[3] not in users:
                    users.add(tokens[3])
                    temp = tokens[0] + " " + tokens[1] + ","
                    for i in range(2, len(tokens)):
                        temp += tokens[i] + ","
                    
                    line = temp + "\n"

                    df.write(line)