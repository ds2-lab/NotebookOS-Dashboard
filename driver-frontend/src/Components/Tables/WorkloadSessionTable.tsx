import { SearchWithAutocomplete } from '@Components/Tables/SearchWithAutocomplete';
import { Session, Workload } from '@Data/Workload';
import {
    Badge,
    Button,
    Card,
    CardBody,
    DescriptionList,
    DescriptionListDescription,
    DescriptionListGroup,
    DescriptionListTerm,
    Flex,
    FlexItem,
    Label,
    MenuToggle,
    MenuToggleElement,
    Pagination,
    Popover,
    Select,
    SelectOption,
    Text,
    Toolbar,
    ToolbarContent,
    ToolbarFilter,
    ToolbarGroup,
    ToolbarItem,
    ToolbarToggleGroup,
    Tooltip,
} from '@patternfly/react-core';
import {
    CopyIcon,
    CpuIcon,
    ErrorCircleOIcon,
    FilterIcon,
    InProgressIcon,
    InfoCircleIcon,
    MemoryIcon,
    OffIcon,
    PendingIcon,
    ResourcesEmptyIcon,
    RunningIcon,
    UnknownIcon,
    WarningTriangleIcon,
} from '@patternfly/react-icons';
import {
    ExpandableRowContent,
    InnerScrollContainer,
    Table,
    Tbody,
    Td,
    Th,
    ThProps,
    Thead,
    Tr,
} from '@patternfly/react-table';
import { GpuIcon, GpuIconAlt2 } from '@src/Assets/Icons';
import { SessionTrainingEventTable } from '@src/Components';
import { RoundToThreeDecimalPlaces, RoundToTwoDecimalPlaces } from '@src/Utils';
import React, { ReactElement } from 'react';

const tableColumns = {
    id: 'ID',
    status: 'Status',
    currentTickNumber: 'Tick',
    completedExecutions: 'Done',
    remainingExecutions: 'Remain',
    deepLearningCategory: 'Category',
    deepLearningModel: 'Model',
    deepLearningDataset: 'Dataset',
    millicpus: 'Millicpus',
    memory: 'RAM (MB)',
    gpus: 'GPUs',
    vram: 'VRAM (GB)',
};

const sessions_table_columns: string[] = [
    'ID',
    'Status',
    'Tick',
    'Done',
    'Remain',
    'Category',
    'Model',
    'Dataset',
    'Millicpus',
    'RAM (MB)',
    'GPUs',
    'VRAM (GB)',
];

const table_columns_no_single_blocks: string[] = [
    'Done',
    'Remain',
    'Category',
    'Model',
    'Dataset',
    'Millicpus',
    'RAM (MB)',
    'GPUs',
    'VRAM (GB)',
];

const sessions_table_column_right_borders: boolean[] = [false, true, false, false, true, false, false, false, false];

const sessions_table_column_blocks: string[][] = [
    ['ID'],
    ['Status'],
    ['Tick'],
    ['Done', 'Remain'],
    ['Category', 'Model', 'Dataset'],
    ['Millicpus', 'RAM (MB)', 'GPUs', 'VRAM (GB)'],
];

const num_blocks_with_one_elem: number = sessions_table_column_blocks.reduce(
    (accumulator, currentValue) => (currentValue.length == 1 ? accumulator + 1 : accumulator),
    0,
);

const sessions_table_column_block_names: string[] = [
    'ID',
    'Status',
    'Tick',
    'Executions',
    'Deep Learning',
    'Resources',
];

/**
 * Return the number of trainings that the given session has left to complete, if that information is available.
 *
 * If that information is not available, then return the string "N/A".
 */
function getRemainingTrainings(session: Session): string | number {
    if (session.trainings) {
        return session.trainings.length - session.trainings_completed;
    }

    return 'N/A';
}

const sessionStatuses: string[] = ['awaiting start', 'idle', 'training_submitted', 'training', 'terminated', 'erred'];

function getStatusLabel(status: string, error_message?: string): ReactElement {
    switch (status) {
        case 'awaiting start':
            return (
                <Tooltip position="right" content="This session has not yet been created or started yet.">
                    <Label icon={<PendingIcon />} color="grey">
                        {status}
                    </Label>
                </Tooltip>
            );
        case 'idle':
            return (
                <Tooltip position="right" content="This session is actively-running, but it is not currently training.">
                    <Label icon={<ResourcesEmptyIcon />} color="blue">
                        {status}
                    </Label>
                </Tooltip>
            );
        case 'training_submitted':
            return (
                <Tooltip position="right" content="This session is about to start actively training.">
                    <Label icon={<InProgressIcon />} color="green">
                        {status}
                    </Label>
                </Tooltip>
            );
        case 'training':
            return (
                <Tooltip position="right" content="This session is actively training.">
                    <Label icon={<RunningIcon />} color="green">
                        {status}
                    </Label>
                </Tooltip>
            );
        case 'terminated':
            return (
                <Tooltip position="right" content="This session has been stopped permanently (without error).">
                    <Label icon={<OffIcon />} color="orange">
                        {status}
                    </Label>
                </Tooltip>
            );
        case 'erred':
            return (
                <Tooltip
                    position="right"
                    content={`This session has been terminated due to an unexpected error: ${error_message}`}
                >
                    <Label icon={<ErrorCircleOIcon />} color="red">
                        {' '}
                        {status}
                    </Label>
                </Tooltip>
            );
        default:
            return (
                <Tooltip position="right" content="This session is in an unknown or unexpected state.">
                    <Label icon={<UnknownIcon />} color="red">
                        {' '}
                        unknown: {status}
                    </Label>
                </Tooltip>
            );
    }
}

function getSessionStatusLabel(session: Session): ReactElement {
    if (session.discarded) {
        return (
            <Tooltip position="right" content="This session was discarded and will not be sampled in this workload.">
                <Label icon={<WarningTriangleIcon />} color="orange">
                    discarded
                </Label>
            </Tooltip>
        );
    }

    return getStatusLabel(session.state, session.error_message);
}

// Since OnSort specifies sorted columns by index, we need sortable values for our object by column index.
// This example is trivial since our data objects just contain strings, but if the data was more complex
// this would be a place to return simplified string or number versions of each column to sort by.
function getSortableRowValues(session: Session): (string | number | Date)[] {
    const {
        id,
        state,
        current_tick_number,
        trainings,
        trainings_completed,
        model_dataset_category,
        assigned_model,
        assigned_dataset,
        current_resource_request,
        max_resource_request,
    } = session;

    let status: string = state;
    if (session.discarded) {
        status = 'discarded';
    }

    return [
        id,
        status,
        current_tick_number,
        trainings_completed,
        trainings.length - trainings_completed,
        model_dataset_category,
        assigned_model,
        assigned_dataset,
        current_resource_request.cpus,
        current_resource_request.memory,
        current_resource_request.gpus,
        max_resource_request.gpus,
        current_resource_request.vram,
    ];
}

export interface WorkloadSessionTableProps {
    children?: React.ReactNode;
    workload: Workload | null;
    showDiscardedSessions?: boolean;
    hasBorders?: boolean;
}

/**
 * Extract and return the abbreviated name from the full name (if an abbreviated name exists). Otherwise,
 * return the full, original name.
 */
function getShortDeepLearningName(name: string): string {
    if (!name) {
        return 'N/A';
    }

    // If the name includes an abbreviation (e.g., "(CV)" or "(CoLA)"), then use the abbreviation.
    if (name.includes('(') && name.includes(')')) {
        const lindex: number = name.lastIndexOf('(');
        const rindex: number = name.lastIndexOf(')');

        return name.substring(lindex + 1, rindex);
    }

    return name;
}

interface OpenFilterMenus {
    model: boolean;
    dataset: boolean;
    category: boolean;
    status: boolean;
    id: boolean;
}

interface Filter {
    model: string[];
    dataset: string[];
    category: string[];
    status: string[];
    id: string[];
}

type FilterType = 'Category' | 'Status' | 'ID' | 'Model' | 'Dataset';

const ModelFilter: FilterType = 'Model';
const DatasetFilter: FilterType = 'Dataset';
const CategoryFilter: FilterType = 'Category';
const StatusFilter: FilterType = 'Status';
const IdFilter: FilterType = 'ID';

// FilterTypes that have an associated dropdown/select menu.
const selectFilterTypes: FilterType[] = [StatusFilter, CategoryFilter, ModelFilter, DatasetFilter];

interface SessionFilterSelectProps {
    filterType: FilterType;
    filters: string[];
    values: string[];
    onSelectFilter: (filterType: FilterType, event: React.MouseEvent | undefined, value: string) => void;
    onDeleteFilter: (filterType: FilterType, value: string) => void;
    onDeleteFilterGroup: (filterType: FilterType) => void;
    open: boolean;
    toggleOpen: (filterType: FilterType, value: boolean) => void;
}

export const SessionFilterSelectMenu: React.FunctionComponent<SessionFilterSelectProps> = (
    props: SessionFilterSelectProps,
) => {
    return (
        <ToolbarFilter
            chips={props.filters}
            deleteChip={(category, chip) => props.onDeleteFilter(category as FilterType, chip as string)}
            deleteChipGroup={(category) => props.onDeleteFilterGroup(category as FilterType)}
            categoryName={props.filterType}
        >
            <Select
                id={`"select-deep-learning-${props.filterType.toLowerCase()}"`}
                aria-label={`Select Deep Learning ${props.filterType}`}
                toggle={(toggleRef: React.Ref<MenuToggleElement>) => (
                    <MenuToggle
                        ref={toggleRef}
                        onClick={(evt: React.MouseEvent) => {
                            evt.preventDefault();
                            evt.stopPropagation();
                            props.toggleOpen(props.filterType, !props.open);
                        }}
                        isExpanded={props.open}
                        isFullWidth
                    >
                        <Flex direction={{ default: 'row' }} spaceItems={{ default: 'spaceItemsSm' }}>
                            <FlexItem>{props.filterType}</FlexItem>
                            <FlexItem>
                                {props.filters.length > 0 && <Badge isRead>{props.filters.length}</Badge>}
                            </FlexItem>
                        </Flex>
                    </MenuToggle>
                )}
                isOpen={props.open}
                onOpenChange={(isOpen: boolean) => props.toggleOpen(props.filterType, isOpen)}
                onSelect={(event?: React.MouseEvent, value?: string | number) =>
                    props.onSelectFilter(props.filterType, event, value as string)
                }
            >
                {props.values.map((value: string) => (
                    <SelectOption hasCheckbox key={value} value={value} isSelected={props.filters.includes(value)}>
                        {props.filterType == StatusFilter ? getStatusLabel(value) : value}
                    </SelectOption>
                ))}
            </Select>
        </ToolbarFilter>
    );
};

// Displays the Sessions from a workload in a table.
export const WorkloadSessionTable: React.FunctionComponent<WorkloadSessionTableProps> = (
    props: WorkloadSessionTableProps,
) => {
    const [page, setPage] = React.useState(1);
    const [perPage, setPerPage] = React.useState(5);

    // Index of the currently sorted column
    const [activeSortIndex, setActiveSortIndex] = React.useState<number | null>(null);

    const [expandedSessions, setExpandedSessions] = React.useState<string[]>([]);

    // Sort direction of the currently sorted column
    const [activeSortDirection, setActiveSortDirection] = React.useState<'asc' | 'desc' | null>(null);

    const [showCopySuccessContent, setShowCopySuccessContent] = React.useState(false);

    const [sortedSessions, setSortedSessions] = React.useState<Session[]>([]);

    const [availDeepLearningCategories, setAvailDeepLearningCategories] = React.useState<string[]>([]);
    const [availableDeepLearningModels, setAvailableDeepLearningModels] = React.useState<string[]>([]);
    const [availableDeepLearningDatasets, setAvailableDeepLearningDatasets] = React.useState<string[]>([]);

    const [openFilterMenus, setOpenFilterMenus] = React.useState<OpenFilterMenus>({
        category: false,
        model: false,
        dataset: false,
        status: false,
        id: false,
    });

    const [filters, setFilters] = React.useState<Filter>({
        category: [],
        model: [],
        dataset: [],
        status: [],
        id: [],
    });

    const onDeleteFilterGroup = (type: FilterType) => {
        if (type === CategoryFilter) {
            setFilters({
                model: filters.model,
                dataset: filters.dataset,
                category: [],
                status: filters.status,
                id: filters.id,
            });
        } else if (type === StatusFilter) {
            setFilters({
                model: filters.model,
                dataset: filters.dataset,
                category: filters.category,
                status: [],
                id: filters.id,
            });
        } else if (type === IdFilter) {
            setFilters({
                model: filters.model,
                dataset: filters.dataset,
                category: filters.category,
                status: filters.status,
                id: [],
            });
        } else if (type === ModelFilter) {
            setFilters({
                model: [],
                dataset: filters.dataset,
                category: filters.category,
                status: filters.status,
                id: filters.id,
            });
        } else if (type === DatasetFilter) {
            setFilters({
                model: filters.model,
                dataset: [],
                category: filters.category,
                status: filters.status,
                id: filters.id,
            });
        }
    };

    const onDeleteFilter = (type: FilterType, id: string) => {
        if (type === CategoryFilter) {
            setFilters({
                category: filters.category.filter((fil: string) => fil !== id),
                status: filters.status,
                id: filters.id,
                model: filters.model,
                dataset: filters.dataset,
            });
        } else if (type === StatusFilter) {
            setFilters({
                category: filters.category,
                status: filters.status.filter((fil: string) => fil !== id),
                id: filters.id,
                model: filters.model,
                dataset: filters.dataset,
            });
        } else if (type === IdFilter) {
            setFilters({
                category: filters.category,
                status: filters.status,
                id: filters.id.filter((fil: string) => fil !== id),
                model: filters.model,
                dataset: filters.dataset,
            });
        } else if (type === ModelFilter) {
            setFilters({
                model: filters.model.filter((fil: string) => fil !== id),
                dataset: filters.dataset,
                category: filters.category,
                status: filters.status,
                id: filters.id,
            });
        } else if (type === DatasetFilter) {
            setFilters({
                model: filters.model,
                dataset: filters.dataset.filter((fil: string) => fil !== id),
                category: filters.category,
                status: filters.status,
                id: filters.id,
            });
        }
    };

    const onSelectFilter = (
        type: FilterType,
        event: React.MouseEvent | React.ChangeEvent | undefined,
        selection: string,
    ) => {
        const checked = (event?.target as HTMLInputElement).checked;
        if (type === CategoryFilter) {
            setFilters({
                category: checked
                    ? [...filters.category, selection]
                    : filters.category.filter((fil: string) => fil !== selection),
                status: filters.status,
                id: filters.id,
                model: filters.model,
                dataset: filters.dataset,
            });
        } else if (type === StatusFilter) {
            setFilters({
                category: filters.category,
                status: checked
                    ? [...filters.status, selection]
                    : filters.status.filter((fil: string) => fil !== selection),
                id: filters.id,
                model: filters.model,
                dataset: filters.dataset,
            });
        } else if (type === ModelFilter) {
            setFilters({
                category: filters.category,
                status: filters.status,
                id: filters.id,
                model: checked
                    ? [...filters.model, selection]
                    : filters.model.filter((fil: string) => fil !== selection),
                dataset: filters.dataset,
            });
        } else if (type === DatasetFilter) {
            setFilters({
                category: filters.category,
                status: filters.status,
                id: filters.id,
                model: filters.model,
                dataset: checked
                    ? [...filters.dataset, selection]
                    : filters.dataset.filter((fil: string) => fil !== selection),
            });
        }
    };

    const filteredSessions: Session[] = React.useMemo<Session[]>(() => {
        const deepLearningCategories: Set<string> = new Set<string>();
        const deepLearningModels: Set<string> = new Set<string>();
        const deepLearningDatasets: Set<string> = new Set<string>();

        const sess =
            props.workload?.sessions.filter((session: Session) => {
                deepLearningCategories.add(session.model_dataset_category);
                deepLearningModels.add(session.assigned_model);
                deepLearningDatasets.add(session.assigned_dataset);

                if (filters.id.length > 0 && !filters.id.includes(session.id)) {
                    return false;
                }

                if (filters.category.length > 0 && !filters.category.includes(session.model_dataset_category)) {
                    return false;
                }

                if (filters.model.length > 0 && !filters.model.includes(session.assigned_model)) {
                    return false;
                }

                if (filters.dataset.length > 0 && !filters.dataset.includes(session.assigned_dataset)) {
                    return false;
                }

                if (filters.status.length > 0 && !filters.status.includes(session.state)) {
                    return false;
                }

                return props.showDiscardedSessions || !session.discarded;
            }) || [];

        setAvailDeepLearningCategories(Array.from(deepLearningCategories).sort());
        setAvailableDeepLearningModels(Array.from(deepLearningModels).sort());
        setAvailableDeepLearningDatasets(Array.from(deepLearningDatasets).sort());

        return sess;
    }, [props.showDiscardedSessions, props.workload?.sessions, filters]);

    React.useEffect(() => {
        let sorted: Session[] = filteredSessions;
        if (activeSortIndex !== null) {
            sorted =
                filteredSessions.sort((a, b) => {
                    const aValue = getSortableRowValues(a)[activeSortIndex];
                    const bValue = getSortableRowValues(b)[activeSortIndex];
                    if (typeof aValue === 'number') {
                        // Numeric sort
                        if (activeSortDirection === 'asc') {
                            return (aValue as number) - (bValue as number);
                        }
                        return (bValue as number) - (aValue as number);
                    } else {
                        // String sort
                        if (activeSortDirection === 'asc') {
                            return (aValue as string).localeCompare(bValue as string);
                        }
                        return (bValue as string).localeCompare(aValue as string);
                    }
                }) || [];

            console.log(`Sessions were sorted: ${sorted}`);
            setSortedSessions(sorted);
        }

        setSortedSessions(sorted);
    }, [filteredSessions, activeSortDirection, activeSortIndex]);

    const copyText: string = 'Copy session ID to clipboard';
    const doneCopyText: string = 'Successfully copied session ID to clipboard!';

    const setSessionExpanded = (session: Session, isExpanding = true) =>
        setExpandedSessions((prevExpanded) => {
            const otherExpandedSessionNames = prevExpanded.filter((r) => r !== session.id);
            return isExpanding ? [...otherExpandedSessionNames, session.id] : otherExpandedSessionNames;
        });
    const isSessionExpanded = (session: Session) => expandedSessions.includes(session.id);

    const onPerPageSelect = (
        _event: React.MouseEvent | React.KeyboardEvent | MouseEvent,
        newPerPage: number,
        newPage: number,
    ) => {
        setPerPage(newPerPage);
        setPage(newPage);
    };

    const getSortParams = (columnIndex: number): ThProps['sort'] => ({
        sortBy: {
            index: activeSortIndex!,
            direction: activeSortDirection!,
            defaultDirection: 'asc', // starting sort direction when first sorting a column. Defaults to 'asc'
        },
        onSort: (_event, index, direction) => {
            setActiveSortIndex(index);
            setActiveSortDirection(direction);
        },
        columnIndex,
    });

    const pagination = (
        <Pagination
            itemCount={sortedSessions.length}
            perPage={perPage}
            page={page}
            perPageOptions={[
                { title: '1 session', value: 1 },
                { title: '2 sessions', value: 2 },
                {
                    title: '3 sessions',
                    value: 3,
                },
                { title: '4 sessions', value: 4 },
                { title: '5 sessions', value: 5 },
                {
                    title: '10 sessions',
                    value: 10,
                },
                { title: '25 sessions', value: 25 },
                { title: '50 sessions', value: 50 },
            ]}
            onSetPage={(_event, newPage: number) => setPage(newPage)}
            onPerPageSelect={onPerPageSelect}
            variant={'bottom'}
            ouiaId="WorkloadSessionsPagination"
        />
    );

    const getSessionPopoverContent = (session: Session) => {
        return (
            <React.Fragment>
                <DescriptionList columnModifier={{ lg: '3Col' }} displaySize={'lg'}>
                    <DescriptionListGroup>
                        <DescriptionListTerm>Start Tick</DescriptionListTerm>
                        <DescriptionListDescription>{session.start_tick}</DescriptionListDescription>
                    </DescriptionListGroup>
                    <DescriptionListGroup>
                        <DescriptionListTerm>Stop Tick</DescriptionListTerm>
                        <DescriptionListDescription>{session.stop_tick}</DescriptionListDescription>
                    </DescriptionListGroup>
                    <DescriptionListGroup>
                        <DescriptionListTerm>Tick</DescriptionListTerm>
                        <DescriptionListDescription>
                            {session.state == 'awaiting start' ? 'Waiting to Start' : session.current_tick_number}
                        </DescriptionListDescription>
                    </DescriptionListGroup>
                </DescriptionList>
            </React.Fragment>
        );
    };

    const getSessionPopoverHeader = (session: Session) => {
        return (
            <Flex direction={{ default: 'row' }} spaceItems={{ default: 'spaceItemsXs' }}>
                <FlexItem>
                    <InfoCircleIcon />
                </FlexItem>
                <FlexItem>Session {session.id}</FlexItem>
            </Flex>
        );
    };

    const tableHead = (
        <Thead noWrap hasNestedHeader>
            {/* The first Tr represents the top level of columns. */}
            {/* Each must pass either rowSpan if the column does not contain sub columns or colSpan if the column contains sub columns. */}
            {/* The value of rowSpan is equal to the number of rows the nested header will span, typically 2, */}
            {/* and the value of colSpan is equal to the number of sub columns in a column. */}
            {/* Each Th except the last should also pass hasRightBorder. */}
            <Tr>
                <Th
                    key={`workload_${props.workload?.id}_column_expand_action_0`}
                    aria-label={`workload_${props.workload?.id}_column_expand_action`}
                    rowSpan={2}
                />
                {sessions_table_column_blocks.map((column_names: string[], blockIndex: number) => (
                    <Th
                        hasRightBorder={blockIndex != sessions_table_column_blocks.length - 1}
                        key={`workload_${props.workload?.id}_column_block_${blockIndex}`}
                        aria-label={`${sessions_table_column_block_names[blockIndex]}-column-block`}
                        colSpan={column_names.length > 1 ? column_names.length : undefined}
                        rowSpan={column_names.length > 1 ? undefined : 2}
                        sort={column_names.length > 1 ? undefined : getSortParams(blockIndex)}
                    >
                        {/*{getTableHeadContent(blockIndex)}*/}
                        {sessions_table_column_block_names[blockIndex]}
                    </Th>
                ))}
            </Tr>
            {/* The second Tr represents the second level of sub columns. */}
            {/* The Th in this row each should pass isSubHeader, and the last sub column of a column should also pass hasRightBorder.  */}
            <Tr resetOffset>
                {table_columns_no_single_blocks.map((column, columnIndex) => (
                    <Th
                        isSubheader
                        hasRightBorder={sessions_table_column_right_borders[columnIndex]}
                        key={`workload_${props.workload?.id}_column_${columnIndex}`}
                        sort={getSortParams(columnIndex + num_blocks_with_one_elem)}
                        aria-label={`${column}-column`}
                    >
                        {column}
                    </Th>
                ))}
            </Tr>
        </Thead>
    );

    const getTableRow = (rowIndex: number): ReactElement | undefined => {
        const session: Session = sortedSessions[rowIndex];

        if (!props.showDiscardedSessions && session.discarded) {
            return undefined;
        }

        // key={`workload_event_${props.workload?.events_processed[0]?.id}_row_${rowIndex}`}
        return (
            <Tbody key={`session-${session.id}-row-${rowIndex}`} isExpanded={isSessionExpanded(session)}>
                <Tr>
                    <Td
                        expand={
                            session.trainings.length > 0
                                ? {
                                      rowIndex,
                                      isExpanded: isSessionExpanded(session),
                                      onToggle: () => setSessionExpanded(session, !isSessionExpanded(session)),
                                      expandId: 'composable-nested-table-expandable-example',
                                  }
                                : undefined
                        }
                    />
                    <Td dataLabel={tableColumns.id}>
                        <Popover
                            alertSeverityVariant="info"
                            headerComponent="h1"
                            position={'right'}
                            hasAutoWidth={true}
                            headerContent={getSessionPopoverHeader(session)}
                            bodyContent={getSessionPopoverContent(session)}
                        >
                            <Text component={'small'} style={{ cursor: 'pointer' }}>
                                {session.id + '  '}
                            </Text>
                        </Popover>
                        <Tooltip
                            content={showCopySuccessContent ? doneCopyText : copyText}
                            position={'right'}
                            entryDelay={75}
                            exitDelay={200}
                            onTooltipHidden={() => setShowCopySuccessContent(false)}
                        >
                            <Button
                                icon={<CopyIcon />}
                                variant={'link'}
                                component={'span'}
                                isInline
                                onClick={async (event) => {
                                    event.preventDefault();
                                    await navigator.clipboard.writeText(session.id);

                                    setShowCopySuccessContent(!showCopySuccessContent);
                                }}
                            />
                        </Tooltip>
                    </Td>
                    <Td dataLabel={tableColumns.status}>{getSessionStatusLabel(session)}</Td>
                    <Td dataLabel={tableColumns.currentTickNumber}>{session.current_tick_number}</Td>
                    <Td dataLabel={tableColumns.completedExecutions}>{session.trainings_completed || '0'}</Td>
                    <Td dataLabel={tableColumns.remainingExecutions}>{getRemainingTrainings(session)}</Td>
                    <Td dataLabel={tableColumns.deepLearningCategory}>
                        <Text component={'small'}>{getShortDeepLearningName(session.model_dataset_category)}</Text>
                    </Td>
                    <Td dataLabel={tableColumns.deepLearningModel}>
                        <Text component={'small'}>{getShortDeepLearningName(session.assigned_model)}</Text>
                    </Td>
                    <Td dataLabel={tableColumns.deepLearningDataset}>
                        <Text component={'small'}>{getShortDeepLearningName(session.assigned_dataset)}</Text>
                    </Td>
                    <Td dataLabel={tableColumns.millicpus}>
                        <CpuIcon />{' '}
                        {session?.current_resource_request
                            ? RoundToThreeDecimalPlaces(session?.current_resource_request.cpus)
                            : 0}
                        {'/'}
                        {RoundToThreeDecimalPlaces(session?.max_resource_request.cpus)}
                    </Td>
                    <Td dataLabel={tableColumns.memory}>
                        <MemoryIcon />
                        {session?.current_resource_request.memory
                            ? RoundToThreeDecimalPlaces(session?.current_resource_request.memory)
                            : 0}
                        {'/'}
                        {RoundToThreeDecimalPlaces(session?.max_resource_request.memory)}
                    </Td>
                    <Td dataLabel={tableColumns.gpus}>
                        <GpuIcon />
                        {session?.current_resource_request.memory
                            ? RoundToTwoDecimalPlaces(session?.current_resource_request.gpus)
                            : 0}
                        {'/'}
                        {RoundToThreeDecimalPlaces(session?.max_resource_request.gpus)}
                    </Td>
                    <Td dataLabel={tableColumns.vram}>
                        <GpuIconAlt2 />
                        {session?.current_resource_request.vram
                            ? RoundToThreeDecimalPlaces(session?.current_resource_request.vram)
                            : 0}
                        {'/'}
                        {RoundToThreeDecimalPlaces(session?.max_resource_request.vram)}
                    </Td>
                </Tr>
                <Tr isExpanded={isSessionExpanded(session)}>
                    <Td dataLabel={`${session.id} expended`} colSpan={sessions_table_columns.length + 1}>
                        <ExpandableRowContent>
                            <SessionTrainingEventTable session={session} isNested={true} isStriped={true} />
                        </ExpandableRowContent>
                    </Td>
                </Tr>
            </Tbody>
        );
    };

    // Indices from current pagination state.
    const startIndex: number = perPage * (page - 1);
    const endIndex: number = perPage * (page - 1) + perPage;

    const getTableRows = () => {
        const tableRows: ReactElement[] = [];
        for (let i: number = startIndex; i < endIndex && i < sortedSessions.length; i++) {
            const tableRow: ReactElement | undefined = getTableRow(i);
            if (tableRow !== undefined) {
                tableRows.push(tableRow);
            }
        }

        return tableRows;
    };

    const onSelectSessionIdFilter = (
        _event: React.MouseEvent<Element, MouseEvent> | undefined,
        value: string | number | undefined,
    ) => {
        if (filters.id.includes(value as string)) {
            setFilters({
                category: filters.category,
                status: filters.status,
                id: filters.id.filter((id) => id !== value),
                model: filters.model,
                dataset: filters.dataset,
            });
        } else {
            setFilters({
                category: filters.category,
                status: filters.status,
                id: [...filters.id, value as string],
                model: filters.model,
                dataset: filters.dataset,
            });
        }
    };

    const getWordsForSearchWithAutocomplete = (): string[] => {
        if (props.showDiscardedSessions) {
            return props.workload?.sessions.map((session: Session) => session.id) || [];
        }

        const sessionIds: string[] | undefined = props.workload?.sessions.reduce(function (
            filtered: string[],
            session: Session,
        ): string[] {
            if (!session.discarded) {
                filtered.push(session.id);
            }
            return filtered;
        }, []);

        return sessionIds || [];
    };

    const sessionIdFilterToolbarItem = (
        <ToolbarFilter
            chips={filters.id}
            deleteChip={(category, chip) => onDeleteFilter(category as FilterType, chip as string)}
            deleteChipGroup={(category) => onDeleteFilterGroup(category as FilterType)}
            categoryName={IdFilter}
        >
            <SearchWithAutocomplete
                words={getWordsForSearchWithAutocomplete()}
                setValue={(value: string) => {
                    if (filters.id.includes(value as string)) {
                        return;
                    }

                    onSelectSessionIdFilter(undefined, value as string);
                }}
            />
        </ToolbarFilter>
    );

    const clearAllFilters = () => {
        setFilters({
            category: [],
            status: [],
            id: [],
            model: [],
            dataset: [],
        });
    };

    const customChipGroupContent = (
        <React.Fragment>
            <ToolbarItem>
                <Button variant={'link'} isInline onClick={() => clearAllFilters()}>
                    Clear all filters
                </Button>
            </ToolbarItem>
        </React.Fragment>
    );

    const getValuesForFilterDropdownMenu = (filterType: FilterType): string[] => {
        if (filterType == StatusFilter) {
            return sessionStatuses;
            // return sessionStatuses.map((status: string) => getStatusLabel(status));
        } else if (filterType == CategoryFilter) {
            return availDeepLearningCategories;
        } else if (filterType == ModelFilter) {
            return availableDeepLearningModels;
        } else if (filterType == DatasetFilter) {
            return availableDeepLearningDatasets;
        }

        console.error(`Unknown or unexpected filter type: ${filterType}`);

        return [];
    };

    const tableToolbar = (
        <Toolbar usePageInsets id="compact-toolbar" customChipGroupContent={customChipGroupContent}>
            <ToolbarContent>
                <ToolbarToggleGroup toggleIcon={<FilterIcon />} breakpoint="xl">
                    {sessionIdFilterToolbarItem}
                    <ToolbarGroup variant={'filter-group'}>
                        {selectFilterTypes.map((filterType: FilterType) => (
                            <SessionFilterSelectMenu
                                key={`select-filter-${filterType}`}
                                filterType={filterType}
                                filters={filters[filterType.toLowerCase()]}
                                values={getValuesForFilterDropdownMenu(filterType)}
                                onSelectFilter={(ft: FilterType, event: React.MouseEvent | undefined, value: string) =>
                                    onSelectFilter(ft, event, value)
                                }
                                onDeleteFilter={(ft: FilterType, value: string) => onDeleteFilter(ft, value)}
                                onDeleteFilterGroup={(ft: FilterType) => onDeleteFilterGroup(ft)}
                                open={openFilterMenus[filterType.toLowerCase()]}
                                toggleOpen={(targetFilterType: FilterType, isOpen: boolean) =>
                                    setOpenFilterMenus((prevOpenMenus: OpenFilterMenus): OpenFilterMenus => {
                                        const openMenus: OpenFilterMenus = Object.assign({}, prevOpenMenus);

                                        // If it is already open, then just close it.
                                        if (!isOpen) {
                                            openMenus[targetFilterType.toLowerCase()] = false;
                                            return openMenus;
                                        }

                                        // Close all the other filter menus and open this one.
                                        selectFilterTypes.forEach((ft: FilterType) => {
                                            // Open the target filter type.
                                            if (targetFilterType === ft) {
                                                openMenus[targetFilterType.toLowerCase()] = true;
                                                console.log(`Opening menu for filter ${targetFilterType}.`);
                                                return;
                                            }

                                            console.log(`Closing menu for filter ${ft}.`);
                                            // Close all the others.
                                            openMenus[ft.toLowerCase()] = false;
                                        });

                                        return openMenus;
                                    })
                                }
                            />
                        ))}
                    </ToolbarGroup>
                </ToolbarToggleGroup>
            </ToolbarContent>
        </Toolbar>
    );

    return (
        <Card isCompact isRounded isFlat>
            <CardBody>
                <InnerScrollContainer>
                    {tableToolbar}
                    <Table gridBreakPoint={''} borders={props.hasBorders} isStriped isExpandable>
                        {tableHead}
                        {sortedSessions.length > 0 && getTableRows()}
                    </Table>
                    {pagination}
                </InnerScrollContainer>
            </CardBody>
        </Card>
    );
};
