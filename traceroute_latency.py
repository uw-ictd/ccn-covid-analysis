import altair
import pandas as pd
from os import listdir

data_prefix = "data/clean/traceroutes/"
keywords_to_remove = [" ms", "(", ")", "\n"]

def make_traceroute_latency_plot(df):
    plot = altair.Chart(df).mark_line().encode(
        x=altair.X("hop",
                   title="hop #",
                   axis=altair.Axis(labels=True)),
        y=altair.Y("latency:Q",
                   title="total latency (ms)"),
        color="unique_name",
    ).properties(
        width=500,
    ).configure_title(
        fontSize=20,
        font='Courier',
        anchor='start',
        color='gray'
    ).show()

def get_traceroute_data():
    data = instantiate_data()
    files = listdir(data_prefix)
    
    for file in files:
        if file.endswith(".txt"):
            parse_traceroute(file, data)

    return data

def instantiate_data():
    columns = ["dataset", "domain", "hop", "ip", "latency", "last_hop", "unique_name"]
    
    data = {}
    for column in columns:
        data[column] = []

    return data

# dataset,domain,hop,ip,latency,last_hop
def parse_traceroute(file, data):
    with open(data_prefix + file) as f:
        lines = f.readlines()
        domain = ""
        for i in range(len(lines)):
            line = lines[i]
            if line.startswith("traceroute to"):
                tokens = line.split()
                domain = tokens[2]
            else:
                line = remove_keywords_from_string(line, keywords_to_remove)
                # TODO: Update this!
                data["dataset"].append("test")
                data["domain"].append(domain)
                data["last_hop"].append(int(i == len(lines) - 1))
                data["unique_name"].append("") # Set to empty string for now; will be updated later

                tokens = line.split()
                latency = 0
                count = 0
                for j in range(len(tokens)):
                    token = tokens[j]
                    if j == 0:
                        data["hop"].append(int(token))
                    elif j == 2:
                        data["ip"].append(token)
                    else:
                        try:
                            latency += float(token)
                            count += 1
                        except:
                            count -= 1
                            latency += 0

                latency /= float(count if count > 0 else 1)
                data["latency"].append(latency if latency > 0 else float("NaN"))

        return data

def remove_keywords_from_string(string, keywords):
    for keyword in keywords:
        string = string.replace(keyword, "")

    return string

if __name__ == "__main__":
    df = pd.read_csv(data_prefix + "traceroutes.csv")
    temp_df = pd.DataFrame(data=get_traceroute_data())
    df = df.append(temp_df)

    # Update the unique name for each row
    for i, row in df.iterrows():
        unique_name = df.at[i, "dataset"] + ", " + df.at[i, "domain"]
        df.at[i,'unique_name'] = unique_name

    print(df)
    df["latency"] = pd.to_numeric(df["latency"])
    df = df.dropna(subset=["latency"])

    make_traceroute_latency_plot(df)