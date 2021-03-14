from datetime import datetime as dt
from datetime import timedelta as td
from uuid import uuid4 # uuid4 gera um UUID aleatório
from uuid import UUID  # UUID é a classe UUID
from dataclasses import dataclass, field



def ts_pp(ts): 
    """
    ----------------------------------------------------------------------
    function ts_pp stands for timestamp Print Pretty

    takes a datetime object, w timestamp data and returns a pretty formated string
    """
    # [:-3] is to trim last 3 digits so milliseconds are shown 
    # with just 3 digits instead of 6
    return ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def ts_pp_to(ts,ndig=3):
    """
    function ts_pp_to stands for timestamp Print Pretty Time Only
    takes a datetime object, w timestamp data and returns a pretty formated string
    with only the time component with ndig digits in the microsseconds part
    ndig accepts values from 0 to 6.
    """
    # [:-ndig] is to trim last ndig digits so milliseconds are shown 
    # with just ndig digits instead of 6
    assert 0 <= ndig <= 6
    if 0 < ndig < 6:
        ndig = 6-ndig
        return ts.strftime("%H:%M:%S.%f")[:-ndig]
    elif ndig == 6:
        return ts.strftime("%H:%M:%S.%f")
    elif ndig == 0:
        return ts.strftime("%H:%M:%S.%f")[:-7]
    
def now_pp_to(ndig=3):
    return ts_pp_to(dt.now(),ndig=ndig)


# icecream debbuger
from icecream import ic
def x(): return f'{now_pp_to(0)} ic| '
ic.configureOutput(includeContext=True,prefix=x)



@dataclass
class Timer():
    """
        Classe Timer

            para testar performance de tempo

            tem 3 metodos: 
                Timer.start() - começa a contar tempo
                Timer.stop() - 
    """
    # TODO: #21 criar every x period, ex.: every 1 min
    # TODO: finalizar documentação 
    def __init__(self):
        self.id:UUID = uuid4()
        self.ts_start: dt = None
        self.ts_stop: dt = None
        self.ts_elapsed: td = None

    def start(self,silent=False):
        """ method: start >> begins counting time 
            can also be used to re-start counting
        """
        self.ts_start = dt.now()
        if not silent: print(f'{ts_pp(self.ts_start)} : start')

    def stop(self,silent=False):
        if self.ts_start is not None:
            self.ts_stop = dt.now()
            self.ts_elapsed = self.ts_stop - self.ts_start
            if not silent: print(f'{ts_pp(self.ts_stop)} : stoped. elapsed time: {self.ts_elapsed}')
        else:
            print("can't stop timer. Gotta start it first! >>> use Timer.start()")

    def elapsed(self):
        if self.ts_start is not None:
            self.ts_elapsed = dt.now() - self.ts_start
            return self.ts_elapsed
        else:
            print("can't print_elapsed. Gotta start timer first! >>> use Timer.start()")

    def print_elapsed(self,msg:str=None):
            msg = '' if msg is None else " |> " + msg
            print(f'{ts_pp(dt.now())} : elapsed {self.elapsed()}{msg}')


    def print_start(self): 
        print(f'{ts_pp(self.ts_start)} : start time')

    def print_stop(self): 
        print(f'{ts_pp(self.ts_stop)} : stop  time')
