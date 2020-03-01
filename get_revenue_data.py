source_file = "./data/clean/transactions.log"
destination_file = "./data/clean/revenue_data.csv"

if __name__ == "__main__":
    # Open transactions log
    with open(source_file, "r") as sf:
        # Open file to write to
        with open(destination_file, "w") as df:
            # Write colunn headers
            df.write("start,price,\n")

            # Write relevant data to file
            for line in sf.readlines():
                tokens = line.strip().split(" ")

                date = tokens[0] + " " + tokens[1] + ","
                price = tokens[len(tokens) - 1] + ","

                df.write(date + price + "\n")