import { WorkloadPreset } from '@Data/Workload';
import { GetPathForFetch } from '@src/Utils/path_utils';
import useSWR, { mutate } from 'swr';

const fetcher = async (input: RequestInfo | URL) => {
    const randNumber: number = Math.floor(Math.random() * 1e9);
    input += `?randNumber=${randNumber}`;

    const init: RequestInit = {
        method: 'GET',
        headers: {
            Authorization: 'Bearer ' + localStorage.getItem('token'),
        },
    };

    const response: Response = await fetch(input, init); // .then((response: Response) => response.json());

    if (!response.ok || response.status != 200) {
        const respError = await response.json();

        throw new Error(`HTTP ${response.status} ${response.statusText}: ${respError['error']}`);
    }

    return await response.json();
};

const api_endpoint: string = GetPathForFetch('api/workload-presets');

export function useWorkloadPresets() {
    const { data, error, isLoading } = useSWR(api_endpoint, fetcher, { refreshInterval: 120000 });

    const workloadPresets: WorkloadPreset[] = data || [];

    return {
        workloadPresets: workloadPresets,
        workloadPresetsAreLoading: isLoading,
        refreshWorkloadPresets: async () => {
            await mutate(api_endpoint);
        },
        isError: error,
    };
}
