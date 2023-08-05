import dask.array
from prefect import flow, task
from prefect_dask import get_async_dask_client


async def test_planetary_computer_task_runner(mock_pc_task_runner):
    @task
    async def compute_dask_sum():
        """Declares two dask arrays and returns sum"""
        a = dask.array.random.normal(size=(1000, 1000), chunks=(100, 100))
        b = dask.array.random.normal(size=(1000, 1000), chunks=(100, 100))

        async with get_async_dask_client(timeout="120s") as client:
            sum = await client.compute(a + b)
        return sum

    @flow(task_runner=mock_pc_task_runner)
    async def dask_flow():
        prefect_future = await compute_dask_sum.submit()
        return await prefect_future.result()

    result = await dask_flow()
    assert result.shape == (1000, 1000)
