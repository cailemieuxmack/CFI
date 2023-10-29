#!/usr/bin/env python

from __future__ import division

import argparse
import random
import sys
import math
import time
import traceback
import multiprocessing
import multiprocessing.pool

from collections import defaultdict
from csv import DictWriter
from itertools import product
from math import ceil
from multiprocessing import cpu_count
from numpy import arange
from pprint import pprint
from copy import deepcopy
from timeout_decorator import timeout

from schedcat.model.tasks import SporadicTask, TaskSystem
from schedcat.generator.tasks import exponential, uniform
from schedcat.generator.tasksets import NAMED_PERIODS, NAMED_UTILIZATIONS
from schedcat.util.storage import storage
import schedcat.sched.edf as edf
import schedcat.model.resources as resources
import schedcat.generator.tasks as tasks
import schedcat.locking.bounds as bounds
from schedcat.util.time import ms2us
# NEW
import schedcat.sched.fp as fp
from numpy import random

# FIXME this is not working for some reason
# I have coppied and pasted this class below to resolve this
#from util.stats import BernoulliEstimator
import scipy.stats

#MIN_SAMPLES = 1000
#MAX_SAMPLES = 100000
MIN_SAMPLES = 100
MAX_SAMPLES = 1000
MAX_CI      = 0.05
CONFIDENCE  = 0.95

# These need to be set from Jay's measurements
# Need milliseconds
#trying microseconds???
#BUFFER_RATE = 20000000000/14687
BUFFER_DELAY = 8500/20000000000
# MEAN = 28.68593064
# STDEV = 22.86616364

# NAMED_CS_LENGTHS = {
#     'short'         : uniform(1, 15),
#     'moderate'      : uniform(1, 100),
#     'long'          : uniform(5, 1280)
# }

# FIXME This is to resolve import error
class BernoulliEstimator(object):
    def __init__(self, default_mean = 0, default_count = 0):
        self.mean = default_mean
        self.count = default_count

    def add_sample(self, sample):
        self.mean = (self.mean*self.count + sample)/(self.count+1)
        self.count+=1

    def confidence_interval(self, confidence = 0.95):
        if self.count == 0:
            return 1
        else:
            zalpha = scipy.stats.t.ppf((1+confidence)/2.0, self.count-1)
            return zalpha * math.sqrt(self.mean*(1-self.mean)/self.count)

#NAMED_CS_LENGTHS = {
#    'small'         : exponential(10,   100,   1000),
#    'moderate'      : uniform(1,    100),
#    'large'         : exponential(100,  1000,  10000),
#    'variant'       : uniform(1, 100000)
#}

# the following two classes are take from 
# http://stackoverflow.com/questions/6974695/python-process-pool-non-daemonic
# without time, you cannot use timeouts within the threads.
class NoDaemonProcess(multiprocessing.Process):
    def _get_daemon(self):
        return False
    def _set_daemon(self, value):
        pass
    daemon = property(_get_daemon, _set_daemon)

class MyPool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess

class ExperimentManager(object):
    def __init__(self, parameters, outfile):
        random.seed(12345)
        self.parameters = parameters
        self.outfile = outfile
        self.out = DictWriter(outfile, parameters.keys()+DesignPoint.metrics)
        self.out.writeheader()
        self.create_design_points()
    
    def valid(self, dp):
        return dp.processors >= dp.sys_util

    def create_design_points(self, shuffle = True):
        self.design_points = filter(self.valid,
            [DesignPoint(**dict(zip(self.parameters.keys(),val)))
                for val in product(*self.parameters.values())])

        if shuffle:
            random.shuffle(self.design_points)

    def run(self, processors, status = True):
        if processors == 1:
            self._run_serial(status)
        else:
            self._run_parallel(processors, status)

        self.finish()

        return # need to return the results here.

    #Useful for debugging purposes.
    def _run_serial(self, status = True):
        rows = map(lambda dp: dp.run(), self.design_points)
        for i, row in enumerate(rows):
            if status and (self.outfile != sys.stdout):
                sys.stderr.write('\rdone {0:%}'.format(i/len(rows)))
            self.out.writerow(row)

    def _run_parallel(self, processors, status = True):
        #pool = Pool(processes = processors)
        pool = MyPool(processes = processors)
        try:
            for i, row in enumerate(pool.imap_unordered(pickleable_function, self.design_points)):
                if status and (self.outfile != sys.stdout):
                    sys.stderr.write('\rdone {0:%}'.format(i/len(self.design_points)))
                self.out.writerow(row)
            pool.close()
        except Exception as e:
            pool.terminate()
            print e
            raise

    def finish(self):
        self.outfile.close()

# for whatever reason, imap_unordered will not take a lambda function, because
# it is not pickleable. This seems stupid.
def pickleable_function(dp):
    return dp.run()

class DesignPoint(object):
    # Update this
    metrics = ["RTA_DELTA","RTA_NORM"]   #["OMLP", "FMLPOblivious", "FMLPAware", "NOLOCK"]

    # a graph which represents which tests can decide a metric on a given run.
    test_graph = {}

    def __init__(self, **levels):
        self.__dict__.update(levels)
        self.levels = levels
        self.data = dict([(m, BernoulliEstimator()) for m in DesignPoint.metrics])

    def run(self):
        while not self.complete():
            org_ts = self.create_task_set()


            # Write some logic to run all your tests here. This is often study
            # specific, as you can exploit domincance results to accelerate
            # things. A classic example, if the task system isn't schedulable
            # without any synchronization, it isn't schedulable with it.
            if True: #fp.rta.is_schedulable(self.processors, org_ts):
                #self.data["RTA_NORM"].add_sample(True)

                tests_remaining = [self.rta_deta, self.rta_norm]      #[self.omlp, self.fmlp_sob, self.fmlp_saware]

                for test in tests_remaining:
                    ts = org_ts.copy()
                    test(ts)
            else:
                for m in DesignPoint.metrics:
                    self.data[m].add_sample(False)

        return dict(self.levels.items() + [(k,v.mean) for (k,v) in self.data.iteritems()])
    
    # New testing functions
    def rta_deta(self, ts):
        def rta(task,higher_prio_tasks, buffer_rate):
            own_demand = task.__dict__.get('prio_inversion', 0) + task.cost
            #hp_jitter = task.__dict__.get('jitter', 0)
            # see if we find a point where the demand is satisfied
            delta = sum([t.cost for t in higher_prio_tasks]) + own_demand
            while delta <= task.deadline:
                #                     Added delay term        + 1 here for pessimism
                demand = own_demand + (((buffer_rate * delta ) + 1) * BUFFER_DELAY)
                for t in higher_prio_tasks:
                    demand += t.cost * int(ceil(delta/ t.period))
                if demand == delta:
                    # yep, demand will be met by time
                    task.response_time = delta #+ task.__dict__.get('jitter', 0)
                    return True
                else:
                    # try again
                    delta = demand
            # if we get here, we didn't converge
            return False
        
        def test():
            buffer_rate = 20000000000/(abs(random.uniform(5,100)))
            #print buffer_rate
            for i, t in enumerate(ts):
                if not rta(t, ts[0:i],buffer_rate):
                    return False
            return True
        res = test()
        self.data["RTA_DELTA"].add_sample(res)
        return res

    def rta_norm(self, ts):
        def rta(task,higher_prio_tasks):
            own_demand = task.__dict__.get('prio_inversion', 0) + task.cost
            #hp_jitter = task.__dict__.get('jitter', 0)
            # see if we find a point where the demand is satisfied
            delta = sum([t.cost for t in higher_prio_tasks]) + own_demand
            while delta <= task.deadline:
                #                     Added delay term        + 1 here for pessimism
                demand = own_demand #+ ((BUFFER_RATE * delta ) + 1) * BUFFER_DELAY
                for t in higher_prio_tasks:
                    demand += t.cost * int(ceil(delta/ t.period))
                if demand == delta:
                    # yep, demand will be met by time
                    task.response_time = delta #+ task.__dict__.get('jitter', 0)
                    return True
                else:
                    # try again
                    delta = demand
            # if we get here, we didn't converge
            return False
        def test():
            for i, t in enumerate(ts):
                if not rta(t, ts[0:i]):
                    return False
            return True
        res = test()
        self.data["RTA_NORM"].add_sample(res)
        return res

    # NEW
    # ---------------------------------------------------------------
    #OLD

    def omlp(self, ts):
        #@timeout(10, use_signals = False)
        def test():
            bounds.apply_global_omlp_bounds(ts, self.processors)
            return edf.is_schedulable(self.processors, ts,
                                     want_baruah = True,
                                     want_rta    = False,
                                     want_ffdbf  = False,
                                     want_load   = False )#,
                                     #want_la     = False)
        #try:
        res = test()
        #except:
        #    res = False
        self.data["OMLP"].add_sample(res)
        return res

    def fmlp_sob(self, ts):
        #@timeout(10, use_signals = False)
        def test():
            bounds.apply_global_fmlp_sob_bounds(ts)
            return edf.is_schedulable(self.processors, ts,
                                     want_baruah = True,
                                     want_rta    = False,
                                     want_ffdbf  = False,
                                     want_load   = False )#,
                                     #want_la     = False)
        #try:
        res = test()
        #except:
        #    res = False

        self.data["FMLPOblivious"].add_sample(res)
        return res

    def fmlp_saware(self, ts):
        #@timeout(10, use_signals = False)
        def test():
            bounds.apply_generalized_fmlp_bounds(ts, self.processors, using_edf=True)
            return edf.is_schedulable(self.processors, ts,
                                     want_baruah = False,
                                     want_rta    = False,
                                     want_ffdbf  = False,
                                     want_load   = False )#,
                                     #want_la     = True)
        #try:
        res = test()
        #except:
        #    res = False

        self.data["FMLPAware"].add_sample(res)
        return res
    
    def complete(self):
        for metric, stat in self.data.iteritems():
            if stat.count < MIN_SAMPLES:
                return False
            elif stat.count > MAX_SAMPLES:
                return True
            elif stat.confidence_interval(CONFIDENCE) > MAX_CI:
                return False
        return True
        
    def create_task_set(self):
        tg = tasks.TaskGenerator(period = NAMED_PERIODS[self.period],
                                    util   = NAMED_UTILIZATIONS[self.task_util])

        ts = tg.make_task_set(max_util = self.sys_util, squeeze = True)#, time_conversion=ms2us)

        resources.initialize_resource_model(ts)
        bounds.assign_edf_preemption_levels(ts)

        # for t in random.sample(ts, int(self.req_perc * len(ts))):
        #     t.resmodel[0].add_request(int(NAMED_CS_LENGTHS[self.cs_length]()))

        for t in ts:
            t.partition = 0
            t.response_time = t.deadline

        return ts

# Helper function to more easily specify parameters
def myrange(start, end, inc):
    return arange(start, end+inc, inc)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', "--outfile", type = argparse.FileType('w'),
                        default = sys.stdout,
                        help = "File to output csv file to")
    parser.add_argument('-p', "--pretend", action='store_true',
                        help = "Only print design point, do not execute")
    parser.add_argument('-m', "--processors", default=cpu_count(), type = int,
                        help="Number of processors to execute on")
    parser.add_argument('-q', "--quiet", action='store_true',
                        help = "Quash all progress and status updates")
    args = parser.parse_args()

    params = storage()

    params.processors = [2, 4]
    params.task_util = NAMED_UTILIZATIONS #['uni-light', 'uni-medium', 'uni-heavy']
    params.period = NAMED_PERIODS #['uni-short', 'uni-moderate', 'uni-long']#, 'uni-broad']
    # params.task_util = ['exp-light', 'exp-medium']
    # params.period = ['uni-moderate']
    params.sys_util = myrange(0.25, 4.0, 0.25)
    #params.cs_length = ['small', 'moderate', 'large', 'variant']
    # params.cs_length = ['moderate', 'long']
    params.req_perc = [0.1, 1.0]

    exp = ExperimentManager(params, args.outfile)

    if args.pretend:
        print exp.design_points
    
    if not args.quiet:
        print "Total design points: ", len(exp.design_points)

    if not args.pretend:
        exp.run(args.processors, not args.quiet)

if __name__ == '__main__':
    main()
