package store

import "regexp"

// Private operational evidence only. Not an eligibility or completion result.
type RefundWorkspaceObservation struct {
	Source    string                `json:"source"`
	Phase     string                `json:"phase"`
	Result    string                `json:"result"`
	Items     []RefundWorkspaceItem `json:"items,omitempty"`
	Truncated bool                  `json:"truncated,omitempty"`
}

type RefundWorkspaceItem struct {
	WorkspaceID           string            `json:"workspace_id,omitempty"`
	Structure             string            `json:"structure"`
	Role                  string            `json:"role,omitempty"`
	TargetTeam            bool              `json:"target_team"`
	IsPersonal            *bool             `json:"is_personal,omitempty"`
	IsDefault             *bool             `json:"is_default,omitempty"`
	HasActiveSubscription *bool             `json:"has_active_subscription,omitempty"`
	IsDeactivated         *bool             `json:"is_deactivated,omitempty"`
	Fields                map[string]string `json:"fields,omitempty"`
}

var workspaceIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,100}$`)
var workspacePlanPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9 _+()-]{0,63}$`)

func (v RefundWorkspaceObservation) sanitized() *RefundWorkspaceObservation {
	v.Source = evidenceEnum(v.Source, "auth_workspaces personal_accounts_check")
	v.Phase = evidenceEnum(v.Phase, "authenticated_before_callback before_team_select before_personal_select")
	v.Result = evidenceEnum(v.Result, "observed missing invalid unavailable not_personal identity_unavailable")
	if v.Source == "" || v.Phase == "" || v.Result == "" {
		return nil
	}
	if v.Source == "personal_accounts_check" && v.Phase != "before_team_select" {
		return nil
	}
	if len(v.Items) > 16 {
		v.Items = v.Items[:16]
		v.Truncated = true
	}
	items := make([]RefundWorkspaceItem, 0, len(v.Items))
	for _, row := range v.Items {
		if v.Source == "personal_accounts_check" && (row.Structure != "personal" || row.TargetTeam) {
			continue
		}
		if !workspaceIDPattern.MatchString(row.WorkspaceID) {
			row.WorkspaceID = ""
		}
		row.Structure = evidenceEnum(row.Structure, "personal workspace team business unknown")
		row.Role = evidenceEnum(row.Role, "owner admin member standard-user user")
		fields := make(map[string]string)
		for _, prefix := range []string{"workspace", "account", "entitlement"} {
			for _, key := range []string{"plan_type", "subscription_plan", "plan_display_name", "plan_name", "subscription_tier", "plan_tier", "tier", "rate_limit_multiplier"} {
				name := prefix + "." + key
				if value := row.Fields[name]; workspacePlanPattern.MatchString(value) {
					fields[name] = value
				}
			}
		}
		row.Fields = fields
		items = append(items, row)
	}
	v.Items = items
	return &v
}
