import { JoinPaths } from '@src/Utils/path_utils';
import { useNavigate } from 'react-router-dom';
import type { NavigateOptions } from 'react-router/dist/lib/context';

function isNumber(value?: string | string[] | number): boolean {
    return value != null && value !== '' && !Array.isArray(value) && !isNaN(Number(value.toString()));
}

function useNavigation() {
    const navigate = useNavigate();

    const doNavigate = (paths: number | string | string[] = '', options?: NavigateOptions) => {
        if (isNumber(paths)) {
            navigate(paths as number);
            return;
        }

        let path: string = process.env.PUBLIC_PATH || '/';
        if (Array.isArray(paths)) {
            path = JoinPaths(path, ...paths);
        } else {
            path = JoinPaths(path, paths as string);
        }

        navigate(path, options);
    };

    return {
        navigate: doNavigate,
    };
}

export default useNavigation;
