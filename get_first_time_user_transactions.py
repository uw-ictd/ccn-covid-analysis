source_file = "./data/clean/transactions.log"
destination_file = "./data/clean/first_time_user_transactions.csv"

if __name__ == "__main__":
    # Open transactions log
    with open(source_file, "r") as sf:
        # Open file to write to
        with open(destination_file, "w") as df:
            # Track all the users you have seen before
            users = set()

            # Write colunn headers
            df.write("start,action,user,amount,price,\n")

            # Write relevant data to file
            for line in sf.readlines():
                tokens = line.strip().split(" ")

                # If the line represents a purchase and this user is a new user,
                # write data to the file
                if tokens[2] == "PURCHASE" and tokens[3] not in users:
                    users.add(tokens[3])
                    temp = tokens[0] + " " + tokens[1] + ","
                    for i in range(2, len(tokens)):
                        temp += tokens[i] + ","
                    
                    line = temp + "\n"

                    df.write(line)