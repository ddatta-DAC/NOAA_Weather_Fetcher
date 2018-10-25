
import math
'''
# Refer : https://carnotcycle.wordpress.com/2012/08/04/how-to-convert-relative-humidity-to-absolute-humidity/
# AH = 6.112 x e^[(17.67 x [T - 273.15] )/( [T - 273.15]+243.5)] x rh x 2.1674 / (T)
# Input :
# Temp in K | float
# RH as % | float
'''

def calc_ah(temp,rh):
    T = temp
    numerator = 6.112 * math.pow( math.e , ((17.67 * (T - 273.15) )/( (T - 273.15) + 243.5)) ) * rh * 2.1674 
    ah = numerator / T
    return ah



