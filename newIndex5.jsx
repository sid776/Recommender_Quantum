// frontend/src/components/elements/DynamicSelect.jsx
import React from "react";
import { Controller, useFormContext } from "react-hook-form";
import AsyncSelect from "react-select/async";

export default function DynamicSelect({
  id,
  fieldName,
  label,
  placeholder,
  dataLoader,           // async () => [{label, value}, ...]
  defaultValue,
  onSelectionChange,    // (option) => void
  disabled = false,
  selectProps = {},     // optional passthrough for react-select props/styles
}) {
  const { control } = useFormContext();

  const loadOptions = async (input) => {
    const res = typeof dataLoader === "function" ? await dataLoader(input) : [];
    return Array.isArray(res) ? res : [];
  };

  const mergedStyles = {
    menuPortal: (base) => ({ ...base, zIndex: 9999 }),
    menu: (base) => ({ ...base, zIndex: 9999 }),
    control: (base) => ({ ...base, minHeight: 36 }),
    ...selectProps.styles,
  };

  return (
    <div className="w-full">
      {label ? (
        <label htmlFor={id || fieldName} className="block text-sm font-medium mb-1">
          {label}
        </label>
      ) : null}

      <Controller
        control={control}
        name={fieldName}
        defaultValue={defaultValue ?? null}
        render={({ field: { onChange, value, ref } }) => (
          <AsyncSelect
            inputId={id || fieldName}
            instanceId={id || fieldName}
            cacheOptions
            defaultOptions
            loadOptions={loadOptions}
            value={value || null}
            isDisabled={disabled}
            isClearable
            placeholder={placeholder || "Select..."}

            /* Ensure the menu overlays AG Grid / other containers */
            menuPortalTarget={typeof document !== "undefined" ? document.body : null}
            menuPosition="fixed"
            menuPlacement="auto"
            menuShouldScrollIntoView={false}
            styles={mergedStyles}

            classNamePrefix="ds"
            className="min-w-[280px]"

            onChange={(opt, meta) => {
              onChange(opt || null);
              if (onSelectionChange) onSelectionChange(opt || null, meta);
            }}

            {...selectProps}
            ref={ref}
          />
        )}
      />
    </div>
  );
}
