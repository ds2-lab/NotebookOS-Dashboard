import { Card, CardBody, Pagination } from '@patternfly/react-core';
import { InnerScrollContainer, Table, Tbody, Td, Th, Thead, Tr } from '@patternfly/react-table';
import { PendingTraining } from '@src/Data';
import { FormatSecondsShort, UnixTimestampToDateString } from '@src/Utils';
import React from 'react';
import { v4 as uuidv4 } from 'uuid';

interface PendingRequestsTableProps {
    pending_trainings?: Map<string, PendingTraining>;
}

const pending_trainings: Map<string, PendingTraining> = new Map();

const training1: PendingTraining = {
    kernel_id: uuidv4(),
    execute_request_id: uuidv4(),
    submitted_at_unix_millis: 1741732621315,
};
pending_trainings.set(training1.kernel_id, training1);

const training2: PendingTraining = {
    kernel_id: uuidv4(),
    execute_request_id: uuidv4(),
    submitted_at_unix_millis: 1741732934495,
};
pending_trainings.set(training2.kernel_id, training2);

export const PendingRequestsTable: React.FunctionComponent<PendingRequestsTableProps> = (
    props: PendingRequestsTableProps,
) => {
    const [page, setPage] = React.useState(1);
    const [perPage, setPerPage] = React.useState(5);

    const onPerPageSelect = (
        _event: React.MouseEvent | React.KeyboardEvent | MouseEvent,
        newPerPage: number,
        newPage: number,
    ) => {
        setPerPage(newPerPage);
        setPage(newPage);
    };

    const pagination = (
        <Pagination
            itemCount={props.pending_trainings?.size}
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

    const tableHead = (
        <Thead noWrap>
            <Tr>
                <Th>Kernel ID</Th>
                <Th>Request ID</Th>
                <Th>Submitted At</Th>
                <Th>Time Elapsed</Th>
            </Tr>
        </Thead>
    );

    const tableBody = (
        <Tbody>
            {props.pending_trainings &&
                Array.from(props.pending_trainings).map((value: [string, PendingTraining], idx: number) => {
                    return (
                        <Tr key={`pending-training-${idx}`}>
                            <Td dataLabel={'Kernel ID'}>{value[0]}</Td>
                            <Td dataLabel={'Request ID'}>{value[1].execute_request_id}</Td>
                            <Td dataLabel={'Submitted At'}>
                                {UnixTimestampToDateString(value[1].submitted_at_unix_millis / 1.0e3, false)}
                            </Td>
                            <Td dataLabel={'Time Elapsed'}>
                                {FormatSecondsShort((Date.now() - value[1].submitted_at_unix_millis) / 1.0e3)}
                            </Td>
                        </Tr>
                    );
                })}
        </Tbody>
    );

    return (
        <Card isCompact isRounded isFlat>
            <CardBody>
                <InnerScrollContainer>
                    <Table gridBreakPoint={''} isStriped isExpandable>
                        {tableHead}
                        {tableBody}
                    </Table>
                    {pagination}
                </InnerScrollContainer>
            </CardBody>
        </Card>
    );
};
