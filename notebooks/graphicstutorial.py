from mspasspy.graphics import wtva_raw
from mspasspy.graphics import image_raw
from mspasspy.graphics import wtvaplot
from mspasspy.graphics import SectionPlotter
from mspasspy.graphics import SeismicPlotter
import numpy as np

import matplotlib.pyplot as plt
import mspasspy.ccore as mspass

def rickerwave(f, dt):
    r"""
    Given a frequency and time sampling rate, outputs ricker function. The
    length of the function varies according to f and dt, in order for the
    ricker function starts and ends as zero. It is also considered that the
    functions is causal, what means it starts at time zero. To satisfy sampling
    and stability:

    .. math::

        f << \frac{1}{2 dt}.

    Here, we consider this as:

    .. math::

        f < 0.2 \frac{1}{2 dt}.

    Parameters:

    * f : float
        dominant frequency value in Hz
    * dt : float
        time sampling rate in seconds (usually it is in the order of ms)

    Returns:

    * ricker : float
        Ricker function for the given parameters.

    """
    assert f < 0.2*(1./(2.*dt)), "Frequency too high for the dt chosen."
    nw = 2.2/f/dt
    nw = 2*int(np.floor(nw/2)) + 1
    nc = int(np.floor(nw/2))
    ricker = np.zeros(nw)
    k = np.arange(1, nw+1)
    alpha = (nc-k+1)*f*dt*np.pi
    beta = alpha**2
    ricker = (1.-beta*2)*np.exp(-beta)
    return ricker
def setbasics(d,n):
    """
    Takes a child of BasicTimeSeries and defines required attributes with
    a common set of putters - attributes are frozen at appropriate values
    for this tutorial.  This function is only to avoid repetitious code
    in other sections.  Note d is altered in place

    :param d:  TimeSeries or Seismogram object or Core versions of same
    :param n:  value use for npts - length of data series
    """
    d.npts=n
    d.set_dt(0.005)
    d.t0=0.0
    d.tref=mspass.TimeReferenceType.Relative
    d.live=True
def makeseis():
    """
    Builds mspass.Seismogram object used in this tutorial.  Components have
    amplitudes scaled by 1, 2, and 3 for 0, 1, and 2 respectively.
    """
    d=mspass.Seismogram()
    setbasics(d,1000)
    y=rickerwave(2.0,0.005)
    ny=len(y)
    # this algorithm only works because calls to set_npts is setbasics
    # initialize data array to all zeros.  Output will not look right if
    # ny>1000 but parameters above assure that isn't so.  Just be careful
    # editing parameters to rickerwave
    for k in range(3):
        for i in range(min(ny,1000)):
            d.u[k,i]=y[i]/float(k+1)
    return d
def makets():
    """
    Build mspass.TimeSeries object used in this tutorial
    """
    d=mspass.TimeSeries()
    setbasics(d,1000)
    y=rickerwave(2.0,0.005)
    ny=len(y)
    for i in range(min(ny,1000)):
        d.s[i]=y[i]
    return d
def maketsens(d,n=20,moveout=True,moveout_dt=0.05):
    """
    Makes a TimeSeries ensemble as copies of d.  If moveout is true
    applies a linear moveout to members using moveout_dt times
    count of member in ensemble.
    """
    # If python had templates this would be one because this and the
    # function below are identical except for types
    result=mspass.TimeSeriesEnsemble()
    for i in range(n):
        y=mspass.TimeSeries(d)  # this makes a required deep copy
        if(moveout):
            y.t0+=float(i)*moveout_dt
        result.member.append(y)
    return result
def makeseisens(d,n=20,moveout=True,moveout_dt=0.05):
    """
    Makes a TimeSeries ensemble as copies of d.  If moveout is true
    applies a linear moveout to members using moveout_dt times
    count of member in ensemble.
    """
    result=mspass.SeismogramEnsemble()
    for i in range(n):
        y=mspass.Seismogram(d)  # this makes a required deep copy
        if(moveout):
            y.t0+=float(i)*moveout_dt
        result.member.append(y)
    return result
