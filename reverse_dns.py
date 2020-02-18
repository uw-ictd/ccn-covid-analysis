import pickle
import lzma
import datetime
import multiprocessing as mp
import socket


def get_dnslog(filename):
    dnslog = dict()
    i = 0
    with lzma.open(filename) as f:
        while True:
            try:
                temp = pickle.load(f)
            except EOFError:
                break
            for addr in temp['response_addresses']:
                if addr not in dnslog.keys():
                    dnslog[addr] = temp['host']
            i += 1
    return dnslog


def reverse_dns(address, dnslog):
    if address in dnslog.keys():
        print(dnslog[address])
        return dnslog[address]
    else:
        temp = socket.gethostbyaddr(str(address))[0]
        for addr in dnslog:
            if dnslog[addr] in temp:
                print("ok???")
                return dnslog[addr]
        dnslog[address] = temp
        print("out")
        return temp


if __name__ == '__main__':
    dnslog = get_dnslog('data/2019-05-17-dnslog_archive-000.xz')
    with open("dnslog.pickle", mode="wb") as f:
        pickle.dump(dnslog, f, protocol = pickle.HIGHEST_PROTOCOL)
