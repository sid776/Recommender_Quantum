import { useForm, FormProvider } from "react-hook-form";
import { useEffect, useState } from "react";
import { DropdownFieldSet, InputFieldset, AppButton, Loader, Toggle, AgGridTable } from "../../elements";
import { getVaRModelByName, useFetch } from "../../../api"
import { Box, Dialog, Tag, Wrap, WrapItem, Collapsible, Skeleton } from "@chakra-ui/react";
import CheckBoxTreeDialog from "./TreeDialog"
import { AgGridReact } from "ag-grid-react";
import 'ag-grid-enterprise';
import { PNL_STRIPS_COLUMN_DEFINITIONS } from "../../elements/TableColumnDefinitions"

export default function Calculator() {
    const methods = useForm();
    const { setValue } = methods;
    const [loading, setLoading] = useState(true);
    const [VaRModels, setVaRModels] = useState([]);
    const [VaREntities, setVaREntities] = useState([]);
    const [editVaRModel, setEditVaRModel] = useState(false);
    const [isDisabled, setIsDisabled] = useState(true);
    const [selectedBooks, setSelectedBooks] = useState([]);
    const [selectedProducts, setSelectedProducts] = useState([]);
    const [open, setOpen] = useState(false)
    const [openTweak, setOpenTweak] = useState(false)
    const [advanceSettings, setAdvanceSettings] = useState(false)
    const [openProducts, setOpenProducts] = useState(false)
    const [tweakEnabled, setTweakEnabled] = useState(false)
    const [PnLStrips, setPnLStrips] = useState([])
    const [runEnabled, setRunEnabled] = useState(true)

    const valuationMethodologyList = [
        { value: 'Full-Reval', label: 'Full-Reval' },
        { value: 'Sensitivity', label: 'Sensitivity' }
    ]

    const statusCodeList = [
        { value: 'Active', label: 'Active' },
        { value: 'Retired', label: 'Retired' },
    ]

    const varTypeList = [
        { value: 1, label: 'REG' },
        { value: 2, label: 'STRESS' },
    ]

    useEffect(() => {
        const loadVaRModels = async () => {
            const data = await useFetch("/varmodel/all")
            setVaRModels(data.models);
            setLoading(false)
        };
        loadVaRModels();
    }, []);

    const onSubmit = async (data) => {
        const fetchPnLStrips = async () => {
            const data = await useFetch("/pnlstrips")
            setPnLStrips(data.details)
            setRunEnabled(false)
        }
        fetchPnLStrips()
    }

    const handleBooks = (checkedItems) => {
        setSelectedBooks(checkedItems);
        setOpen(false);
    };

    const handleProducts = (checkedItems) => {
        setSelectedProducts(checkedItems);
        setOpenProducts(false);
    };

    const loadVaRModelDetails = (varModelData, is_reset = false) => {
        setValue("confidence_level", Object.keys(varModelData).length ? varModelData.confidence_level : "");
        setValue("holding_period", !is_reset ? varModelData.holding_period : "");
        setValue("time_horizon_period", !is_reset ? varModelData.time_horizon : "");
        setValue("status_cd", !is_reset ? varModelData.status_code : null);
        setValue("var_type", !is_reset ? varModelData.var_type : null);
    }

    const handleOpenChange = (e) => {
        setOpen(e.open)
    }
    const handleOpenProductChange = (e) => {
        setOpenProducts(e.open)
    }

    const handleOpenTweakChange = (e) => {
        setOpenTweak(e.open)
    }

    const handleCobDateSelection = (e) => {
        console.log(e.target.value)
    }

    const handleSelection = (selectedOption) => {
        if (!selectedOption) {
            loadVaRModelDetails(true);
            setEditVaRModel(false);
            return;
        }
        const loadVaRModelData = async () => {
            const varModelData = await getVaRModelByName(selectedOption.label)
            // const entities = await getEntities()
            // setVaREntities(entities)
            // varModelData['entities'] = entities
            // setVaRModelDetails(varModelData);
            loadVaRModelDetails(varModelData)
            setIsDisabled(false)
            // if (VaRModelObject && VaRModelObject.label && cobDate) {
            //     if (VaRModelObject.label.includes("REG")) {
            //         methods.setValue('time_series_date', cobDate)
            //     }
            //     if (VaRModelObject.label.includes("STRESS")) {
            //         methods.setValue('time_series_date', "2008-12-31")
            //     }
            // }
        }
        loadVaRModelData()
    };

    const getBoxWithTags = (records) => {

        return (
            <Box
                className="px-1 bg-gray-50 border border-gray-200 block w-full leading-8 outline-smbc-green text-sm disabled:text-black rounded-md min-h-[2.5rem] max-h-[10rem] overflow-y-auto"
            >
                <Wrap spacing={2} className="pt-1 pb-1">
                    {records.map((record, index) => (
                        <WrapItem key={index}>
                            <Tag.Root className="bg-orange-200 pt-1 pb-1 pr-2 pl-2 shadow-md" rounded="full">
                                <Tag.Label className="text-sm">{record.join(" ➔ ")}</Tag.Label>
                            </Tag.Root>
                        </WrapItem>
                    ))}
                </Wrap>
            </Box>
        )
    }

    return (
        <>
            <Box className="overflow-auto" height="calc(100vh - 70px)">
                <FormProvider {...methods}>
                    <form
                        onSubmit={methods.handleSubmit(onSubmit)}
                        className="flex flex-col gap-3 "
                    >
                        <div className="p-4 bg-white shadow-md rounded-lg">
                            <div className="flex gap-4 pb-3">
                                <a className="text-lg font-bold cursor-pointer" onClick={() => { setRunEnabled(!runEnabled) }}>
                                    <span>Run Parameters</span>
                                </a>
                                <i
                                    className={`transition-transform duration-300 ph ph-caret-down ${runEnabled ? "rotate-180" : "rotate-0"
                                        }`}
                                />
                            </div>
                            <Collapsible.Root open={runEnabled} unmountOnExit>
                                <Collapsible.Content>
                                    <div className="flex gap-4 pb-3">
                                        <InputFieldset
                                            id='date'
                                            label='COB'
                                            fieldName='date'
                                            tooltipMsg='Date'
                                            type='date'
                                            onChange={handleCobDateSelection}
                                            registerOptions={{
                                                required: 'required'
                                            }}
                                            style={{ "width": "auto" }}

                                        />
                                    </div>
                                    <div className="flex gap-4 pb-2">
                                        <fieldset className="w-full relative pt-1">
                                            <div className="flex gap-1 text-sm">
                                                <label className="font-semibold flex gap-1" id="books">Portifolio</label>
                                                <Dialog.Root lazyMount scrollBehavior="inside" open={open} onOpenChange={handleOpenChange}>
                                                    <Dialog.Trigger asChild>
                                                        <i className="cursor-pointer ph ph-bold ph-arrow-square-out text-[#004832]" />
                                                    </Dialog.Trigger>
                                                    <CheckBoxTreeDialog
                                                        name='Books'
                                                        onSave={handleBooks}
                                                        initialChecked={selectedBooks}
                                                    />
                                                </Dialog.Root>
                                            </div>
                                            {/* {getDialog("Portifolio", open, handleOpenChange, handleBooks, selectedBooks)} */}
                                            {getBoxWithTags(selectedBooks)}
                                        </fieldset>
                                        <fieldset className="w-full relative pt-1">
                                            <div className="flex gap-1 text-sm">
                                                <label className="font-semibold flex gap-1" id="products">Products</label>
                                                <Dialog.Root lazyMount scrollBehavior="inside" open={openProducts} onOpenChange={handleOpenProductChange}>
                                                    <Dialog.Trigger asChild>
                                                        <i className="cursor-pointer ph ph-bold ph-arrow-square-out text-[#004832]" />
                                                    </Dialog.Trigger>
                                                    <CheckBoxTreeDialog
                                                        name='Products'
                                                        onSave={handleProducts}
                                                        initialChecked={selectedProducts}
                                                    />
                                                </Dialog.Root>
                                            </div>
                                            {/* {getDialog("Products", openProducts, handleOpenProductChange, handleProducts, selectedProducts)} */}
                                            {getBoxWithTags(selectedProducts)}
                                        </fieldset>
                                    </div>
                                    <div className="flex gap-4 pb-3">
                                        <DropdownFieldSet
                                            id='var-model'
                                            label='Import VaR Model'
                                            fieldName='var_model'
                                            optionsData={VaRModels}
                                            errorMessage='required'
                                            isSimpleSelect={true}
                                            isCreatable={false}
                                            onSelectionChange={handleSelection}
                                            required
                                            style={{ "width": "50%" }}
                                        />
                                        <div className="flex items-center mt-[23px]">
                                            <Toggle
                                                fieldName={"tweak"}
                                                label={"Tweak"}
                                                onSwitchChange={setTweakEnabled}

                                            />
                                        </div>
                                    </div>
                                    <Collapsible.Root open={tweakEnabled} unmountOnExit>
                                        <Collapsible.Content>
                                            <>
                                                <div className="flex gap-4 pb-2">
                                                    <InputFieldset
                                                        id='time_series_end_date'
                                                        label='Time Series End Date'
                                                        fieldName='time_series_date'
                                                        tooltipMsg='Time Series End Date'
                                                        type='date'
                                                    // registerOptions={{
                                                    //     required: 'required'
                                                    // }}
                                                    />
                                                    <DropdownFieldSet
                                                        id='var-type'
                                                        label='VaR Type'
                                                        fieldName='var_type'
                                                        tooltipMsg='VaR Type'
                                                        required={false}
                                                        errorMessage='required'
                                                        isSimpleSelect={true}
                                                        isCreatable={false}
                                                        optionsData={varTypeList}
                                                    />
                                                    <InputFieldset
                                                        id='confidence-level'
                                                        label='Confidence Level'
                                                        fieldName='confidence_level'
                                                        tooltipMsg='Confidence Level'
                                                        placeholder="value should be either 95 or 99"
                                                        type='text'
                                                    // registerOptions={{
                                                    //     required: 'required'
                                                    // }}
                                                    />
                                                    <InputFieldset
                                                        id='holding-period'
                                                        label='Holding Period'
                                                        fieldName='holding_period'
                                                        tooltipMsg='Holding Period'
                                                    // registerOptions={{ required: 'required' }}
                                                    />
                                                    <InputFieldset
                                                        id='time-horizon-period'
                                                        label='Time Horizon Period'
                                                        fieldName='time_horizon_period'
                                                        tooltipMsg='Time Horizon Period'
                                                    // registerOptions={{ required: 'required' }}
                                                    />
                                                </div>
                                                <div className="flex gap-4 pb-2">
                                                    <DropdownFieldSet
                                                        id='entities'
                                                        label='Entities'
                                                        fieldName='entities'
                                                        optionsData={VaREntities}
                                                        errorMessage='required'
                                                        isSimpleSelect={true}
                                                        isCreatable={false}
                                                        isMulti={true}
                                                        onSelectionChange={handleSelection}

                                                    />
                                                    <InputFieldset
                                                        id='purpose'
                                                        label='Purpose/Name'
                                                        fieldName='purpose'
                                                        tooltipMsg='Purpose'
                                                        placeholder="Reason or Name for this workflow"
                                                        type='text'
                                                    // registerOptions={{
                                                    //     required: 'required'
                                                    // }}
                                                    />
                                                </div>
                                            </>
                                        </Collapsible.Content>
                                    </Collapsible.Root>
                                    <div className="flex gap-4 pb-3">
                                        <a className="text-sm text-blue-800 cursor-pointer" onClick={() => { setAdvanceSettings(!advanceSettings) }}>
                                            <span className="underline">Advance Settings</span>
                                        </a>
                                        <i
                                            className={`transition-transform duration-300 ph ph-caret-down ${advanceSettings ? "rotate-180" : "rotate-0"
                                                }`}
                                        />
                                    </div>
                                    <Collapsible.Root open={advanceSettings} unmountOnExit>
                                        <Collapsible.Content>
                                            <div className="flex gap-4 pb-3 transition-all transition-discrete">
                                                <InputFieldset
                                                    id='snapshot'
                                                    label='VaR Snapshot'
                                                    fieldName='snapshot'
                                                    tooltipMsg='Snapshot'
                                                    placeholder=""
                                                    type='text'
                                                    // registerOptions={{
                                                    //     required: 'required'
                                                    // }}
                                                    options={{
                                                        defaultValue: "EOD"
                                                    }}
                                                />
                                                <InputFieldset
                                                    id='input-snapshot'
                                                    label='Sensitivity Snapshot'
                                                    fieldName='input_snapshot'
                                                    tooltipMsg='Input Snapshot'
                                                    placeholder=""
                                                    type='text'
                                                    // registerOptions={{
                                                    //     required: 'required'
                                                    // }}
                                                    options={{
                                                        defaultValue: "EOD"
                                                    }}
                                                />
                                                <DropdownFieldSet
                                                    id='var-type'
                                                    label='Valuation Methodology'
                                                    fieldName='var_type'
                                                    optionsData={valuationMethodologyList}
                                                    // required
                                                    errorMessage='required'
                                                    isSimpleSelect={true}
                                                    isCreatable={false}
                                                />
                                            </div>
                                        </Collapsible.Content>
                                    </Collapsible.Root>
                                    <div className="flex justify-between">
                                        <div>
                                            <AppButton
                                                name="action"
                                                value="CANCEL"
                                                variant="text"
                                                onClick={() => resetForm(true)}
                                            >
                                                Cancel
                                            </AppButton>
                                        </div>
                                        <div className="flex space-x-4">
                                            <AppButton
                                                name="action"
                                                value="PREVIEW"
                                                variant="secondary"
                                                onClick={() => resetForm(true)}
                                            >
                                                {/* <svg className="mr-3 size-5 animate-spin text-green-900" style={{color: "green"}} fill="currentColor" viewBox="0 0 24 24"></svg> */}
                                                Preview
                                            </AppButton>

                                            <AppButton
                                                name="action"
                                                value="RUN"
                                                type="submit"
                                            >
                                                Run
                                            </AppButton>
                                        </div>
                                    </div>
                                </Collapsible.Content>
                            </Collapsible.Root>
                        </div>
                    </form>

                </FormProvider>
                {PnLStrips.length > 0 &&
                    <Box style={{ height: "600px", width: "100%" }} className="ag-theme-alpine rounded-lg shadow-md pt-3">
                        <AgGridReact
                            rowData={PnLStrips}
                            rowSelection="multiple"
                            loading={loading}
                            columnDefs={PNL_STRIPS_COLUMN_DEFINITIONS}
                            defaultColDef={{
                                flex: 1,
                                minWidth: 130,
                                enableValue: true,
                                enableRowGroup: true,
                                enablePivot: true,
                                enableCharts: true
                            }}
                            autoGroupColumnDef={{
                                minWidth: 200,
                                pinned: "left",
                            }}
                            pivotMode={true}
                            cellSelection={true}
                            sideBar={"columns"}
                            pivotPanelShow={"always"}
                            enableRangeSelection={true}
                            getContextMenuItems={(params) => {
                                const defaultItems = params.defaultItems;
                                const customItems = ['chartRange', 'pivotChart'];
                                return [...defaultItems, 'separator', ...customItems];
                            }}
                        />
                    </Box>}
            </Box>
        </>
    )
}
