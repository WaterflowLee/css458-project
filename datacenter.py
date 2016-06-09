""" datacenter with catastrophically failing hard drives """
from SimPy.Simulation import *
from SimPy.SimPlot import *
from random import expovariate, weibullvariate, seed

## Model components -----------------------------


class HardDrive(Process):
    """ HardDrive fails, is serviced and gets back to work """
    numFailed = 0
    downTime = Monitor(name="Dead Drives", ylab="Time to Replace")
    
    def __init__(self, ident, size):
        Process.__init__(self, name="{0:04d}".format(ident))
        self.ident = ident
        self.size = size

    def run(self, res, sto, pri):
        while True:
            ttf = weibullvariate(alpha, beta)   # time until failure
            yield hold, self, ttf
            failure = now()                     # failure time
            HardDrive.numFailed += 1
            HardDrive.downTime.observe(HardDrive.numFailed)
            if HardDrive.numFailed == critLevel:
                critical.signal()
                HardDrive.numFailed = 0
                HardDrive.downTime.observe(HardDrive.numFailed)
            else:
                yield waitevent, self, critical
            yield get, self, sto, 1
            yield request, self, res, pri
            self.size = self.got[0].capacity
            ttr = meanTtr + expovariate(1.0 / meanTtr)
            yield hold, self, ttr
            yield release, self, res
            #wait = now() - failure              # waiting time
            #HardDrive.downTime.observe(wait)


class Spare(Lister):
    def __init__(self, cap=1.0):
        self.capacity = cap


class Upgrade(Process):
    curSize = 0.0
    
    def __init__(self, initSize):
        Process.__init__(self, name="Sweeper")
        Upgrade.curSize = initSize

    def check(self):
        while True:
            yield hold, self, upgTime
            Upgrade.curSize += upgAmt


class Supply(Process):
    def __init__(self, stockInt):
        Process.__init__(self, name="Delivery")
        stockInt = stockInt
    
    def restock(self, sto):
        while True:
            yield hold, self, stockInt
            if sto.nrBuffered < sto.capacity:
                order = []
                for i in range(sto.capacity - sto.nrBuffered):
                    order.append(Spare(Upgrade.curSize))
                yield put, self, sto, order


## Experiment data ------------------------------


maxTime   = 87660.0     # system hours to run (87660 = 10 years)
rvCount   = 1000        # number of replicated volumes
rvCap     = 10          # number of drives per replicated volume
onHand    = 300         # number of spare drives in storage
critLevel = 200         # number of failed drives that triggers replacement
initSize  = 1.0         # initial capacity of all drives, in TB
alpha     = 50000.0     # Weibull param
beta      = 2.0         # Weibull param
meanTtr   = .012        # mean time to replace a drive, in hours
upgTime   = 12960.0     # hours between drive upgrades (12960 = 18 months)
upgAmt    = 1.0         # amount by which to upgrade drives, in TB
stockInt  = 168.0       # time between restocking (168 = 1 week)
theSeeds  = [12345, 54321, 99999, 1]
critical  = SimEvent("Drives Critical")


## Model  ---------------------------------------


""" generates a list of hard drives """
def generate(repVols, resource, store):
    output = []
    for i in range(repVols):
        curRv = []
        for j in range(rvCap):
            idNum = i * rvCap + j
            disk = HardDrive(ident=idNum, size=initSize)
            priority = repVols * rvCap - idNum
            activate(disk, disk.run(res=resource, sto=store, pri=priority))
            curRv.append(disk)
        output.append(curRv)
    return output

def model(runSeed):
    seed(runSeed)
    stockBuf = []
    for i in range(onHand):
        stockBuf.append(Spare(initSize))

    tech = Resource(name="Maintenance", unitName="Technician",
                    qType=PriorityQ, preemptable=True, monitored=True)
    stock = Store(name="Stock Room", unitName="Hard Drive",
                  capacity=onHand, initialBuffered=stockBuf, monitored=True)

    initialize()
    HardDrive.downTime.reset()
    HardDrive.downTime.observe(HardDrive.numFailed)
    sweeper = Upgrade(initSize=initSize)
    activate(sweeper, sweeper.check())
    delivery = Supply(stockInt)
    activate(delivery, delivery.restock(sto=stock))
    dataCenter = generate(repVols=rvCount, resource=tech, store=stock)
    simulate(until=maxTime)
    totalCap = 0
    for i in dataCenter:
        for j in i:
            totalCap += j.size
    print "\nTotal capacity: {0:0.1f}\n".format(totalCap)
    print "Drives waiting on tech during replacement:"
    print "  Count: {0}".format(tech.waitMon.count())
    print "  Total: {0}".format(tech.waitMon.total())
    print "  Mean:  {0}".format(tech.waitMon.mean())
    print "  Var:   {0}".format(tech.waitMon.var())
    print "  TAve:  {0}".format(tech.waitMon.timeAverage())
    print "Drives in the stock room:"
    print "  Count: {0}".format(stock.bufferMon.count())
    print "  Total: {0}".format(stock.bufferMon.total())
    print "  Mean:  {0}".format(stock.bufferMon.mean())
    print "  Var:   {0}".format(stock.bufferMon.var())
    print "  TAve:  {0}".format(stock.bufferMon.timeAverage())
    print "Dead drives waiting for replacment cycle:"
    print "  Count: {0}".format(HardDrive.downTime.count())
    print "  Total: {0}".format(HardDrive.downTime.total())
    print "  Mean:  {0}".format(HardDrive.downTime.mean())
    print "  Var:   {0}".format(HardDrive.downTime.var())
    print "  TAve:  {0}".format(HardDrive.downTime.timeAverage())


## Experiment/Result  ---------------------------

for aSeed in theSeeds:
    model(aSeed)
