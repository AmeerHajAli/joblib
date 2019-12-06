import ray
from .parallel import AutoBatchingMixin, ParallelBackendBase

class RayBackend(ParallelBackendBase, AutoBatchingMixin):
    """
        Ray backend uses ray, a system for scalable distributed computing.
        more info are available here: https://ray.readthedocs.io/
    """
    supports_timeout = True
    # not sure if these should be here
    #uses_threads = True
    #supports_sharedmem = True
    #supports_inner_max_num_threads = True
    def __init__(self,**ray_kwargs):
        
        self.ray_kwargs = ray_kwargs
        self.task_futures = set()

    def effective_n_jobs(self, n_jobs):
        """Determine the number of jobs/workers which are going to run in parallel"""
        if n_jobs <= 0:
            raise ValueError('n_jobs <= 0 in ray Parallel has no meaning')
        return n_jobs

    def get_nested_backend(self):
        return RayBackend(ray_kwargs=self.ray_kwargs), -1

    def configure(self, n_jobs=1, parallel=None, **backend_args):
        n_jobs = self.effective_n_jobs(n_jobs)
        if n_jobs == 1:
            # Avoid unnecessary overhead and use sequential backend instead.
            raise FallbackToBackend(
                SequentialBackend(nesting_level=self.nesting_level))
        ray.init(num_cpus = n_jobs, **self.ray_kwargs)
        self._n_jobs = n_jobs
        return n_jobs
    
    def apply_async(self, func, callback=None):
        """Schedule a func to be run"""
        f,args,kwargs = func.items[0]
        #registering the function
        ray_f = ray.remote(lambda *args, **kwargs: f(*args,**kwargs))
        rayfuture = ray_f.remote(*args,**kwargs)
        self.task_futures.add(rayfuture)

        if callback is not None:    
            callback(rayfuture)
            self.task_futures.remove(rayfuture)
        
        # patch to make AsyncResult API work 
        class Future:
            def __init__(self,rayfuture):
                self.rayfuture = rayfuture
            def get(self,timeout=None):
                if timeout:
                    done_futures,remaining_futures = ray.wait([self.rayfuture],1,timeout)
                    if not done_futures:
                        raise TimeoutError()
                result = ray.get(self.rayfuture)
                return [result]

        return Future(rayfuture)
    
    def abort_everything(self,ensure_ready=True):
        ray.shutdown()
        self.task_futures.clear()
        if ensure_ready:
            self.configure(n_jobs=self._n_jobs)
