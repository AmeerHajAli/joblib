import ray
from joblib._parallel_backends import MultiprocessingBackend
from joblib.pool import MemmappingPool, PicklingPool
from ray.experimental.multiprocessing.pool import Pool
class RayBackend(MultiprocessingBackend):

    """Ray backend uses ray, a system for scalable distributed computing.
    more info are available here: https://ray.readthedocs.io
    """
    def configure(self, n_jobs=1, parallel=None, prefer=None, require=None,
                  **memmappingpool_args):
        #Make Ray Pool the father class of PicklingPool 
        PicklingPool.__bases__ = (Pool,) # 
        MemmappingPool.__bases__= (PicklingPool,)
        #all cluster resources
        if n_jobs == -1: 
           n_jobs = int(ray.state.cluster_resources()["CPU"]) 
        eff_n_jobs = super(RayBackend,self).configure(n_jobs, parallel, prefer,require,**memmappingpool_args)
        return eff_n_jobs 
