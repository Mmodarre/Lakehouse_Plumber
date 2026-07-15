"use client"

import { cva } from "class-variance-authority"
import { ToggleGroup } from "radix-ui"

import { cn } from "@/lib/utils"

export type SegmentedControlOption = {
  value: string
  label: string
  disabled?: boolean
}

export type SegmentedControlProps = {
  value: string
  onValueChange: (value: string) => void
  options: SegmentedControlOption[]
  size?: "sm" | "md"
  "aria-label": string
}

const segmentedControlVariants = cva(
  "inline-flex w-fit items-center rounded-lg border bg-muted text-muted-foreground",
  {
    variants: {
      size: {
        md: "h-9 gap-1 p-[3px]",
        sm: "h-7 gap-0.5 p-0.5",
      },
    },
    defaultVariants: {
      size: "md",
    },
  }
)

const segmentedControlItemVariants = cva(
  "inline-flex flex-1 items-center justify-center gap-1.5 rounded-md border border-transparent font-medium whitespace-nowrap text-muted-foreground transition-all hover:text-foreground focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50 focus-visible:outline-1 focus-visible:outline-ring disabled:pointer-events-none disabled:opacity-50 data-[state=on]:bg-card data-[state=on]:font-semibold data-[state=on]:text-foreground data-[state=on]:shadow-sm",
  {
    variants: {
      size: {
        md: "px-3 py-1 text-sm",
        sm: "px-2 py-0.5 text-xs",
      },
    },
    defaultVariants: {
      size: "md",
    },
  }
)

function SegmentedControl({
  value,
  onValueChange,
  options,
  size = "md",
  "aria-label": ariaLabel,
}: SegmentedControlProps) {
  return (
    <ToggleGroup.Root
      type="single"
      value={value}
      // Radix clears the value to "" when the active item is re-pressed. A
      // discriminator must never go empty, so only forward truthy values —
      // this keeps the control single-select and non-deselectable.
      onValueChange={(v) => {
        if (v) onValueChange(v)
      }}
      aria-label={ariaLabel}
      data-slot="segmented-control"
      className={cn(segmentedControlVariants({ size }))}
    >
      {options.map((option) => (
        <ToggleGroup.Item
          key={option.value}
          value={option.value}
          disabled={option.disabled}
          data-slot="segmented-control-item"
          className={cn(segmentedControlItemVariants({ size }))}
        >
          {option.label}
        </ToggleGroup.Item>
      ))}
    </ToggleGroup.Root>
  )
}

export { SegmentedControl, segmentedControlVariants, segmentedControlItemVariants }
