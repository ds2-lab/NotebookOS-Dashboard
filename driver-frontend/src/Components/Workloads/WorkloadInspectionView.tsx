import {
    Button,
    Checkbox,
    DescriptionList,
    DescriptionListDescription,
    DescriptionListGroup,
    DescriptionListTerm,
    Flex,
    FlexItem,
} from '@patternfly/react-core';
import {
    ClipboardCheckIcon,
    ClockIcon,
    CodeIcon,
    DiceIcon,
    MonitoringIcon,
    StopwatchIcon,
    TaskIcon,
} from '@patternfly/react-icons';
import { WorkloadEventTable, WorkloadSessionTable } from '@src/Components';
import { Session, Workload } from '@src/Data';
import { GetToastContentWithHeaderAndBody } from '@src/Utils/toast_utils';
import { RoundToNDecimalPlaces, RoundToTwoDecimalPlaces } from '@Utils/utils';
import { uuidv4 } from 'lib0/random';
import React from 'react';
import toast, { Toast } from 'react-hot-toast';

interface IWorkloadInspectionViewProps {
    workload: Workload;
    showTickDurationChart: boolean;
}

/**
 * Returns a random number between min (inclusive) and max (exclusive)
 */
function getRandomArbitrary(min: number, max: number): number {
    return Math.random() * (max - min) + min;
}

export const WorkloadInspectionView: React.FunctionComponent<IWorkloadInspectionViewProps> = (
    props: IWorkloadInspectionViewProps,
) => {
    const [currentTick, setCurrentTick] = React.useState<number>(0);

    const [tickIdToastId, setTickIdToastId] = React.useState<string | undefined>(undefined);

    // Map from workload ID to the largest tick for which we've shown a toast notification
    // about the workload being incremented to that tick.
    const showedTickNotifications = React.useRef<Map<string, number>>(new Map<string, number>());

    const [showDiscardedEvents, setShowDiscardedEvents] = React.useState<boolean>(false);
    const [showDiscardedSessions, setShowDiscardedSessions] = React.useState<boolean>(false);
    const [workloadSessionTableHasBorders, setWorkloadSessionTableHasBorders] = React.useState<boolean>(false);

    const shouldShowTickNotification = (workloadId: string, tick: number): boolean => {
        if (!showedTickNotifications || !showedTickNotifications.current) {
            return false;
        }

        const lastTickNotification: number = showedTickNotifications.current.get(workloadId) || -1;

        return tick > lastTickNotification;
    };

    /**
     * Return the total number of non-disabled sessions (unless we're showing disabled sessions).
     */
    const getTotalNumSessions = (): number => {
        return props.workload.sessions.reduce((sum: number, session: Session) => {
            if (showDiscardedSessions || !session.discarded) {
                return sum + 1;
            }

            return sum;
        }, 0);
    };

    // TODO: This will miscount the first tick as being smaller, basically whenever we first open the workload
    //       preview to when the next tick begins, it'll count that block as the duration of the first tick,
    //       which is wrong.
    React.useEffect(() => {
        if (props.workload && props.workload?.statistics.current_tick > currentTick) {
            setCurrentTick(props.workload.statistics.current_tick);

            if (shouldShowTickNotification(props.workload.id, props.workload.statistics.current_tick)) {
                const tick: number = props.workload?.statistics.current_tick;
                const toastId: string = toast.custom(
                    (t: Toast) =>
                        GetToastContentWithHeaderAndBody(
                            'Tick Incremented',
                            `Workload ${props.workload?.name} has progressed to Tick #${tick}.`,
                            'info',
                            () => {
                                toast.dismiss(t.id);
                            },
                        ),
                    { icon: '⏱️', style: { maxWidth: 700 }, duration: 5000, id: tickIdToastId || uuidv4() },
                );

                if (tickIdToastId === undefined) {
                    setTickIdToastId(toastId);
                }

                showedTickNotifications.current.set(props.workload.id, tick);
            }
        }
    }, [currentTick, props.workload, props.workload.statistics.current_tick, tickIdToastId]);

    const getTimeElapsedString = () => {
        if (
            props.workload?.statistics.workload_state === undefined ||
            props.workload?.statistics.workload_state === ''
        ) {
            return 'N/A';
        }

        return props.workload?.statistics.time_elapsed_str;
    };

    const randomizeSessionStates = () => {
        const sessions: Session[] = props.workload.sessions;

        for (let i = 0; i < sessions.length; i++) {
            const session = sessions[i];
            session.current_resource_request.cpus = Math.floor(
                getRandomArbitrary(0, session.max_resource_request.cpus),
            );
            session.current_resource_request.memory = getRandomArbitrary(0, session.max_resource_request.memory);
            session.current_resource_request.gpus = Math.floor(
                getRandomArbitrary(0, session.max_resource_request.gpus),
            );
            session.current_resource_request.vram = getRandomArbitrary(0, session.max_resource_request.vram);

            session.trainings_completed = Math.floor(getRandomArbitrary(0, session.trainings.length));

            session.current_tick_number = Math.floor(getRandomArbitrary(0, session.stop_tick));
        }
    };

    return (
        <Flex direction={{ default: 'column' }} spaceItems={{ default: 'spaceItemsXl' }}>
            <Flex
                direction={{ default: 'row' }}
                spaceItems={{ default: 'spaceItemsNone' }}
                alignItems={{ default: props.showTickDurationChart ? 'alignItemsCenter' : 'alignItemsFlexStart' }}
                justifyContent={{
                    default: props.showTickDurationChart ? 'justifyContentCenter' : 'justifyContentFlexStart',
                }}
            >
                <FlexItem>
                    <DescriptionList columnModifier={{ lg: '3Col' }} displaySize={'lg'}>
                        <DescriptionListGroup>
                            <DescriptionListTerm>
                                Seed <DiceIcon />
                            </DescriptionListTerm>
                            <DescriptionListDescription>{props.workload?.seed}</DescriptionListDescription>
                        </DescriptionListGroup>
                        <DescriptionListGroup>
                            <DescriptionListTerm>
                                Time Adjustment Factor <ClockIcon />
                            </DescriptionListTerm>
                            <DescriptionListDescription>
                                {RoundToNDecimalPlaces(props.workload?.timescale_adjustment_factor, 6)}
                            </DescriptionListDescription>
                        </DescriptionListGroup>
                        <DescriptionListGroup>
                            <DescriptionListTerm>
                                Sessions Sample Percentage <TaskIcon />
                            </DescriptionListTerm>
                            <DescriptionListDescription>
                                {props.workload.statistics.sessions_sample_percentage || 1.0}
                            </DescriptionListDescription>
                        </DescriptionListGroup>
                        <DescriptionListGroup>
                            <DescriptionListTerm>
                                Events Processed <MonitoringIcon />
                            </DescriptionListTerm>
                            <DescriptionListDescription>
                                {props.workload?.statistics.num_events_processed}
                            </DescriptionListDescription>
                        </DescriptionListGroup>
                        <DescriptionListGroup>
                            <DescriptionListTerm>
                                Training Events Completed <CodeIcon />
                            </DescriptionListTerm>
                            <DescriptionListDescription>
                                {props.workload?.statistics.num_tasks_executed}
                            </DescriptionListDescription>
                        </DescriptionListGroup>
                        <DescriptionListGroup>
                            <DescriptionListTerm>
                                Time Elapsed <StopwatchIcon />
                            </DescriptionListTerm>
                            <DescriptionListDescription>{getTimeElapsedString()}</DescriptionListDescription>
                        </DescriptionListGroup>
                    </DescriptionList>
                </FlexItem>
            </Flex>
            <FlexItem>
                <Flex direction={{ default: 'row' }}>
                    <FlexItem align={{ default: 'alignLeft' }}>
                        <ClipboardCheckIcon /> {<strong>Events Processed:</strong>}{' '}
                        {props.workload?.statistics.num_events_processed}
                    </FlexItem>
                    <FlexItem align={{ default: 'alignRight' }}>
                        <Checkbox
                            label="Show Discarded Events"
                            id={'show-discarded-events-checkbox'}
                            isChecked={showDiscardedEvents}
                            onChange={(_event: React.FormEvent<HTMLInputElement>, checked: boolean) =>
                                setShowDiscardedEvents(checked)
                            }
                        />
                    </FlexItem>
                </Flex>
                <WorkloadEventTable workload={props.workload} showDiscardedEvents={showDiscardedEvents} />
            </FlexItem>
            <FlexItem>
                <Flex direction={{ default: 'row' }}>
                    <FlexItem align={{ default: 'alignLeft' }}>
                        <ClipboardCheckIcon /> {<strong>Sessions: </strong>}
                        {props.workload?.statistics.num_sessions_created} / {getTotalNumSessions()} (
                        {RoundToTwoDecimalPlaces(
                            100 * (props.workload?.statistics.num_sessions_created / getTotalNumSessions()),
                        ) + '%'}
                        ) created, {props.workload?.statistics.num_active_trainings} actively training
                    </FlexItem>
                    <FlexItem align={{ default: 'alignRight' }}>
                        <Flex direction={{ default: 'row' }}>
                            <Button variant="link" icon={<DiceIcon />} onClick={() => randomizeSessionStates()}>
                                Randomize Session States
                            </Button>
                            <Checkbox
                                label="Row Borders"
                                id={'row-borders-toggle-checkbox'}
                                isChecked={workloadSessionTableHasBorders}
                                onChange={(_event: React.FormEvent<HTMLInputElement>, checked: boolean) =>
                                    setWorkloadSessionTableHasBorders(checked)
                                }
                            />
                            <Checkbox
                                label="Show Discarded Sessions"
                                id={'show-discarded-sessions-checkbox'}
                                isChecked={showDiscardedSessions}
                                onChange={(_event: React.FormEvent<HTMLInputElement>, checked: boolean) =>
                                    setShowDiscardedSessions(checked)
                                }
                            />
                        </Flex>
                    </FlexItem>
                </Flex>
                <WorkloadSessionTable
                    workload={props.workload}
                    showDiscardedSessions={showDiscardedSessions}
                    hasBorders={workloadSessionTableHasBorders}
                />
            </FlexItem>
        </Flex>
    );
};
