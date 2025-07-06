import { Flex, FlexItem, Switch, Tooltip } from '@patternfly/react-core';
import { BlueprintIcon, ClockIcon, CodeIcon, CubeIcon, DiceIcon, TaskIcon } from '@patternfly/react-icons';
import useNavigation from '@Providers/NavigationProvider';
import { GetWorkloadStatusLabel, GetWorkloadStatusTooltip, IsWorkloadFinished, Workload } from '@src/Data';
import { WorkloadContext } from '@src/Providers';
import { RoundToNDecimalPlaces } from '@src/Utils';
import React from 'react';

interface IWorkloadDescriptiveIcons {
    workload: Workload;
}

export const WorkloadDescriptiveIcons: React.FunctionComponent<IWorkloadDescriptiveIcons> = (
    props: IWorkloadDescriptiveIcons,
) => {
    const { toggleDebugLogs } = React.useContext(WorkloadContext);
    const { navigate } = useNavigation();

    return (
        <Flex className="props.workload-descriptive-icons" spaceItems={{ default: 'spaceItemsMd' }}>
            <FlexItem>
                <Tooltip content={GetWorkloadStatusTooltip(props.workload)} position="bottom">
                    <React.Fragment>{GetWorkloadStatusLabel(props.workload)}</React.Fragment>
                </Tooltip>
            </FlexItem>
            <FlexItem>
                <Tooltip content={'Workload Template.'} position="bottom">
                    <React.Fragment>
                        <BlueprintIcon /> &quot;
                        {'Workload Template'}&quot;
                    </React.Fragment>
                </Tooltip>
            </FlexItem>
            <FlexItem>
                <Tooltip content={'Workload seed.'} position="bottom">
                    {/* <Label icon={<DiceIcon />}>{props.workload.seed}</Label> */}
                    <React.Fragment>
                        <DiceIcon /> {props.workload.seed}
                    </React.Fragment>
                </Tooltip>
            </FlexItem>
            <FlexItem>
                <Tooltip content={'Timescale Adjustment Factor.'} position="bottom">
                    <React.Fragment>
                        <ClockIcon /> {RoundToNDecimalPlaces(props.workload.timescale_adjustment_factor, 6)}
                    </React.Fragment>
                </Tooltip>
            </FlexItem>
            <FlexItem>
                <Tooltip content={'Sessions Sample Percentage.'} position="bottom">
                    <React.Fragment>
                        <TaskIcon /> {props.workload.statistics.sessions_sample_percentage || 1.0}
                    </React.Fragment>
                </Tooltip>
            </FlexItem>
            <FlexItem>
                <Tooltip content={`Total number of Sessions involved in the workload.`} position="bottom">
                    <React.Fragment>
                        <CubeIcon /> {props.workload?.sessions.length}
                    </React.Fragment>
                </Tooltip>
            </FlexItem>
            <FlexItem>
                <Tooltip
                    content={`Total number of training events across all Sessions in the workload.`}
                    position="bottom"
                >
                    <React.Fragment>
                        <CodeIcon /> {props.workload?.statistics.total_num_training_events}
                    </React.Fragment>
                </Tooltip>
            </FlexItem>

            <FlexItem align={{ default: 'alignRight' }} alignSelf={{ default: 'alignSelfFlexEnd' }}>
                <Switch
                    id={'props.workload-' + props.workload.id + '-debug-logging-switch'}
                    isDisabled={IsWorkloadFinished(props.workload)}
                    label={'Debug logging'}
                    aria-label="debug-logging-switch"
                    isChecked={props.workload.debug_logging_enabled}
                    ouiaId="DebugLoggingSwitch"
                    onClick={(e) => {
                        e.stopPropagation();
                        e.preventDefault();
                    }}
                    onChange={() => {
                        toggleDebugLogs(props.workload.id, !props.workload.debug_logging_enabled);
                    }}
                />
            </FlexItem>
        </Flex>
    );
};

export default WorkloadDescriptiveIcons;
