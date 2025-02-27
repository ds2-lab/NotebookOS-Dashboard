import { WorkloadInspectionView } from '@Components/Workloads/WorkloadInspectionView';
import { Button, Flex, FlexItem, Modal, ModalVariant, Title, TitleSizes, Tooltip } from '@patternfly/react-core';
import { ArrowRightIcon, CloseIcon, CopyIcon, ExportIcon, PlayIcon, StopIcon } from '@patternfly/react-icons';
import { AuthorizationContext } from '@Providers/AuthProvider';
import useNavigation from '@Providers/NavigationProvider';
import { GetWorkloadStatusLabel, IsInProgress, IsReadyAndWaiting, Workload } from '@src/Data/Workload';
import { WorkloadContext } from '@src/Providers';
import React from 'react';

export interface InspectWorkloadModalProps {
    children?: React.ReactNode;
    isOpen: boolean;
    onClose: () => void;
    workload: Workload;
}

export const InspectWorkloadModal: React.FunctionComponent<InspectWorkloadModalProps> = (props) => {
    const { authenticated } = React.useContext(AuthorizationContext);

    const { exportWorkload, startWorkload, stopWorkload } = React.useContext(WorkloadContext);
    const [showCopySuccessContent, setShowCopySuccessContent] = React.useState<boolean>(false);

    const { navigate } = useNavigation();

    React.useEffect(() => {
        // Automatically close the modal of we are logged out.
        if (!authenticated) {
            props.onClose();
        }
    }, [props, authenticated]);

    const header = (
        <React.Fragment>
            <Title headingLevel="h1" size={TitleSizes['2xl']}>
                {`Workload ${props.workload?.name} `}

                {GetWorkloadStatusLabel(props.workload)}
            </Title>
            <Flex direction={{ default: 'row' }} spaceItems={{ default: 'spaceItemsXs' }}>
                <FlexItem>
                    <Title headingLevel="h3">{props.workload?.id}</Title>
                </FlexItem>
                <FlexItem>
                    <Tooltip
                        content={
                            showCopySuccessContent ? 'Copied successfully workload ID' : 'Copy workload ID to clipboard'
                        }
                        position={'right'}
                        entryDelay={75}
                        exitDelay={200}
                        onTooltipHidden={() => setShowCopySuccessContent(false)}
                    >
                        <Button
                            variant={'link'}
                            isInline
                            icon={<CopyIcon />}
                            onClick={async () => {
                                await navigator.clipboard.writeText(props.workload?.id);

                                setShowCopySuccessContent(true);
                            }}
                        />
                    </Tooltip>
                </FlexItem>
            </Flex>
        </React.Fragment>
    );

    return (
        <Modal
            variant={ModalVariant.large}
            titleIconVariant={'info'}
            header={header}
            aria-label="inspect-workload-modal"
            isOpen={props.isOpen}
            width={1500}
            maxWidth={1920}
            onClose={props.onClose}
            actions={[
                <Button
                    key="start-workload-button"
                    variant="primary"
                    aria-label={'Start workload'}
                    icon={<PlayIcon />}
                    onClick={() => {
                        if (props.workload) {
                            startWorkload(props.workload);
                        }
                    }}
                    isDisabled={!IsReadyAndWaiting(props.workload) || !authenticated}
                >
                    Start Workload
                </Button>,
                <Button
                    key="stop-workload-button"
                    variant="danger"
                    aria-label={'Stop workload'}
                    icon={<StopIcon />}
                    onClick={() => {
                        if (props.workload) {
                            stopWorkload(props.workload);
                        }
                    }}
                    isDisabled={!IsInProgress(props.workload) || !authenticated}
                >
                    Stop Workload
                </Button>,
                <Button
                    key="export_workload_state_button"
                    aria-label={'Export workload state'}
                    variant="secondary"
                    icon={<ExportIcon />}
                    onClick={() => {
                        if (props.workload) {
                            exportWorkload(props.workload);
                        }
                    }}
                >
                    Export
                </Button>,
                <Button
                    key="goto-inspect-workload-page-button"
                    variant="secondary"
                    aria-label={'Go to workload page'}
                    icon={<ArrowRightIcon />}
                    onClick={() => {
                        props.onClose();
                        navigate('/workload/' + props.workload?.id);
                        // navigate('/workloads', { state: { workload: props.workload } });
                    }}
                >
                    Go to Workload Page
                </Button>,
                <Button
                    key="close-inspect-workload-modal-button"
                    variant="secondary"
                    aria-label={'Inspect workload'}
                    icon={<CloseIcon />}
                    onClick={props.onClose}
                >
                    Close Window
                </Button>,
            ]}
        >
            <WorkloadInspectionView workload={props.workload} showTickDurationChart={false} />
        </Modal>
    );
};
