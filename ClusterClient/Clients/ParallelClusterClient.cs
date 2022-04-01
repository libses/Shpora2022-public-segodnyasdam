using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClusterClient.Clients;
using log4net;

namespace ClusterTests;

public class ParallelClusterClient : ClusterClientBase
{
	public ParallelClusterClient(string[] replicaAddresses)
		: base(replicaAddresses)
	{
	}

	public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
	{
		var tasks = new HashSet<Task<string>>();
		foreach (var uri in ReplicaAddresses)
        {
			var webRequest = CreateRequest(uri + "?query=" + query);
			Log.InfoFormat($"Processing {webRequest.RequestUri}");

			var innerTask = ProcessRequestAsync(webRequest);
			tasks.Add(innerTask);
		}

		var nullTask = Task.Delay(timeout).ContinueWith(t => "null");
		tasks.Add(nullTask);
		while (tasks.Count > 1)
        {
			var resultTask = await Task.WhenAny(tasks);
			if (resultTask == nullTask)
				throw new TimeoutException();
			
			if (resultTask.IsCompletedSuccessfully)
			    return resultTask.Result;

			tasks.RemoveWhere(t => t.IsFaulted);
		}

		throw new TimeoutException();
	}

	protected override ILog Log => LogManager.GetLogger(typeof(ParallelClusterClient));

	private static async Task<string> GetGoodTask(List<Task<string>> tasks)
    {
		while (true)
        {
			foreach (var task in tasks)
            {
				if (task.IsCompletedSuccessfully)
                {
					return task.Result;
                }
            }
        }
    }
}