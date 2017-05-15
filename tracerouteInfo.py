import numpy as np
import calendar
from matplotlib import pylab as plt
from pymongo import MongoClient
from collections import defaultdict


class tracerouteInfo:


    def __init__(self,timeBins, hops, suffix=""):
        """ 
Should be called with UTC dates, for example: [datetime(2017,5,1,12,0,tzinfo=pytz.UTC), datetime(2017,5,1,14,0,tzinfo=pytz.UTC)]
        """

        self.data = defaultdict(list)
        self.timeBins = timeBins
        self.binSize = 3600
        self.hops = hops
        self.pltFileSuffix = suffix


    def loadData(self, host="mongodb-iijlab", port=27017, db="atlas", username="", password=""):
        """ Get data from mongodb.
        """
        if username and password:
            mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
            conn = MongoClient(mongo_uri)
        else:
            conn = MongoClient(host, port)

        for t in self.timeBins:
            collection = "traceroute_%04d_%02d_%02d" % (t.year, t.month, t.day)
            cond =[ {"timestamp": {"$gte": calendar.timegm(t.utctimetuple()), "$lt": calendar.timegm(t.utctimetuple())+self.binSize}} ]
            for h in self.hops:
                cond.append({"result": {"$elemMatch": {"result": {"$elemMatch": {"from":h}}}}})

            curr = conn[db][collection].find( {"$and": cond} )

            for trace in curr:
                self.data[t].append(trace)


    def plotRtt(self ):
        bins = np.linspace(0, 500, 100)
        for h in self.hops:
            plt.figure()
            plt.title(h)
            for t in self.timeBins:
                traceroute = [hop["result"] for res in self.data[t] for hop in res["result"]]
                rtts = [elem["rtt"] for hop in traceroute for elem in hop if "from" in elem and elem["from"] == h]
                plt.hist(rtts, bins, histtype="step", label="%02d:30" % t.hour)

            plt.xlabel("RTT")
            plt.ylabel("Number of samples")
            plt.legend()
            plt.tight_layout()
            plt.savefig("fig/%s%s_rtt_distribution.pdf" % (self.pltFileSuffix, h) )


    def plotPathLen(self ):
        bins = np.linspace(0, 30, 30)
        plt.figure()

        for t in self.timeBins:
            pathLen = [len(trace["result"]) for trace in self.data[t] ]
            plt.hist(pathLen, bins, histtype="step", label="%02d:30" % t.hour)

        plt.xlabel("Number of hops")
        plt.ylabel("Number of traceroutes")
        plt.legend(loc="best")
        plt.tight_layout()
        plt.savefig("fig/%spath_len_distribution.pdf" % (self.pltFileSuffix))


    def printStats(self):
        for t,d in self.data.iteritems():
            print("%s: " % (t))
            print("\t %s traceroutes" % (len(d)))

            probes = [res["prb_id"] for res in self.data[t]]
            msms = [res["msm_id"] for res in self.data[t]]
            dsts = [res["dst_addr"] for res in self.data[t]]

            print("\t %s unique probes" % len(np.unique(probes)))
            print("\t %s measurement IDs" % len(np.unique(msms)))
            print("\t %s " % (np.unique(msms)))
            print("\t %s dest IPs" % len(np.unique(dsts)))
            print("\t %s " % (np.unique(dsts)))

        
