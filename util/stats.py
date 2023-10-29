from __future__ import division
import scipy.stats
from math import sqrt

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
            return zalpha * sqrt(self.mean*(1-self.mean)/self.count)
